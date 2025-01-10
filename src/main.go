package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ----------------------------------------------------------------------
// Data Structures
// ----------------------------------------------------------------------

type NexusSearchItem struct {
    Group    string       `json:"group"`
    Name     string       `json:"name"`
    Version  string       `json:"version"`
    Assets   []NexusAsset `json:"assets"`
}

type NexusAsset struct {
    ID          string            `json:"id"`
    Repository  string            `json:"repository"`
    Format      string            `json:"format"`
    Path        string            `json:"path"`
    DownloadURL string            `json:"downloadUrl"`
    Checksum    map[string]string `json:"checksum"` // e.g. {"sha1":"abc123...", "sha256":"def..."}
}

type NexusSearchResponse struct {
    Items             []NexusSearchItem `json:"items"`
    ContinuationToken string            `json:"continuationToken"`
}

type BlobStore struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ----------------------------------------------------------------------
// 1) Define a manual mapping of repo -> groups
//    If a repo is in this map, we'll only use these groups.
//    Otherwise, we fall back to the auto-discovery code.
// ----------------------------------------------------------------------
var manualRepoGroups = map[string][]string{
    // Example override:
    // "maven-releases": {"com.example", "org.apache", "net.foo"},
    // "some-other-repo": {"com.mycompany", "com.thirdparty"},
    // Add your own repo -> []string list as needed
}

// ----------------------------------------------------------------------
// Command-line flags & shared HTTP client
// ----------------------------------------------------------------------
var (
    sourceURL   = flag.String("source-url", getEnv("SOURCE_URL", ""), "Base URL of the source Nexus server (e.g., https://source-nexus:8081)")
    sourceUser  = flag.String("source-user", getEnv("SOURCE_USER", ""), "Username for the source Nexus server")
    sourcePass  = flag.String("source-pass", getEnv("SOURCE_PASS", ""), "Password for the source Nexus server")
    sourceRepo  = flag.String("source-repo", getEnv("SOURCE_REPO", ""), "Repository name on the source server (e.g., raw-repo)")

    targetURL   = flag.String("target-url", getEnv("TARGET_URL", ""), "Base URL of the target Nexus server (e.g., https://target-nexus:8081)")
    targetUser  = flag.String("target-user", getEnv("TARGET_USER", ""), "Username for the target Nexus server")
    targetPass  = flag.String("target-pass", getEnv("TARGET_PASS", ""), "Password for the target Nexus server")
    targetRepo  = flag.String("target-repo", getEnv("TARGET_REPO", ""), "Repository name on the target server (e.g., raw-repo)")

    concurrency = flag.Int("concurrency", getEnvAsInt("CONCURRENCY", 8), "Number of concurrent workers for transfer")

    // You can tweak the Timeout if you're handling extremely large files
    client = &http.Client{
        Timeout: 30 * time.Minute, // shared HTTP client with global timeout
    }
)

// ----------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------
func main() {
    flag.Parse()

    // If target flags are not provided, default to source flags
    if *targetUser == "" {
        *targetUser = *sourceUser
    }
    if *targetPass == "" {
        *targetPass = *sourcePass
    }
    if *targetRepo == "" {
        *targetRepo = *sourceRepo
    }

    // Validate required flags
    if *sourceURL == "" || *sourceRepo == "" || *targetURL == "" || *targetRepo == "" {
        log.Fatal("Error: Required flags are not provided. --source-url, --source-repo, --target-url, --target-repo are mandatory.")
    }

    log.Printf("Starting Nexus transfer\n")
    log.Printf("Source: %s (repo: %s)\n", *sourceURL, *sourceRepo)
    log.Printf("Target: %s (repo: %s)\n", *targetURL, *targetRepo)

	// 0. Ensure the required blob store and repository exists
    // ----------------------------------------------------------------------
	log.Printf("Ensuring target repository (and corresponding blob store) '%s' exists...\n", *targetRepo)
	if err := createOrEnsureRepo(*sourceURL, *sourceRepo, *targetURL, *targetRepo, *sourceUser, *sourcePass, *targetUser, *targetPass); err != nil {
		log.Fatalf("Failed to ensure target repository: %v", err)
	}

    // 1. Get the effective groups for this repo
    // ----------------------------------------------------------------------
    log.Println("Retrieving groups for this repository...")
    groups, err := getGroupsForRepo(*sourceURL, *sourceRepo, *sourceUser, *sourcePass)
    if err != nil {
        log.Fatalf("Failed to get groups: %v", err)
    }
    log.Printf("Found %d groups to process.\n", len(groups))

    // ----------------------------------------------------------------------
    // 2. Create a channel and a wait group for concurrency
    // ----------------------------------------------------------------------
    itemChan := make(chan NexusSearchItem)
    wg := &sync.WaitGroup{}

    // 3. Start workers
    for i := 0; i < *concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for item := range itemChan {
                if err := processArtifact(item); err != nil {
                    // processArtifact may return an error if at least one asset fails
                    // but we continue so other items don't get blocked
                    log.Printf("[ERROR in processArtifact] %v\n", err)
                }
            }
        }()
    }

    // ----------------------------------------------------------------------
    // 4. Query artifacts by each group and send items to the channel
    // ----------------------------------------------------------------------
    for _, group := range groups {
        items, err := queryArtifactsByGroup(group)
        if err != nil {
            log.Printf("[ERROR] Failed to query artifacts for group %s: %v\n", group, err)
            continue
        }
        for _, item := range items {
            itemChan <- item
        }
    }
    close(itemChan)

    // 5. Wait for all workers to finish
    wg.Wait()

    log.Println("Transfer complete.")
}

// ----------------------------------------------------------------------
// createOrEnsureBlobStore: Ensures the required blob store exists on the target
// ----------------------------------------------------------------------
func createOrEnsureBlobStore(baseURL, blobStoreName, user, pass string) error {
	// Check if the blob store already exists
	blobStores, err := listBlobStores(baseURL, user, pass)
	if err != nil {
		return fmt.Errorf("failed to list blob stores: %w", err)
	}

	for _, blobStore := range blobStores {
		if blobStore.Name == blobStoreName {
			log.Printf("Blob store '%s' already exists.\n", blobStoreName)
			return nil
		}
	}

	// Create the blob store if it does not exist
	log.Printf("Creating blob store '%s'...\n", blobStoreName)
	return createBlobStore(baseURL, blobStoreName, user, pass)
}

func listBlobStores(baseURL, user, pass string) ([]BlobStore, error) {
	endpoint := fmt.Sprintf("%s/service/rest/v1/blobstores", strings.TrimRight(baseURL, "/"))

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	if user != "" && pass != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to list blob stores: status %d, body: %s", resp.StatusCode, string(body))
	}

	var blobStores []BlobStore
	if err := json.NewDecoder(resp.Body).Decode(&blobStores); err != nil {
		return nil, fmt.Errorf("decoding blob stores JSON: %w", err)
	}

	return blobStores, nil
}

func createBlobStore(baseURL, blobStoreName, user, pass string) error {
	endpoint := fmt.Sprintf("%s/service/rest/v1/blobstores/file", strings.TrimRight(baseURL, "/"))

	payload := map[string]interface{}{
		"name": blobStoreName,
		"path": blobStoreName,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal blob store payload: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("creating POST request failed: %w", err)
	}
	if user != "" && pass != "" {
		req.SetBasicAuth(user, pass)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST blob store creation error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create blob store: status %d, body: %s", resp.StatusCode, string(body))
	}

	log.Printf("Blob store '%s' successfully created.\n", blobStoreName)
	return nil
}

// ----------------------------------------------------------------------
// createOrEnsureRepo: Ensures the target repository exists by fetching the
// source repository configuration, cleaning it, and creating it on the target
// ----------------------------------------------------------------------
func createOrEnsureRepo(sourceBaseURL, sourceRepo, targetBaseURL, targetRepo, sourceUser, sourcePass, targetUser, targetPass string) error {
	log.Printf("Fetching repository configuration for '%s' from source...\n", sourceRepo)
	srcDef, err := getRepositoryDefinition(sourceBaseURL, sourceRepo, sourceUser, sourcePass)
	if err != nil {
		return fmt.Errorf("failed to fetch repository definition: %w", err)
	}

	// Determine the repository type
	repoType, ok := srcDef["type"].(string)
	if !ok {
		return fmt.Errorf("unable to determine repository type for '%s'", sourceRepo)
	}

	// Determine the blob store name based on the repository type
	blobStoreName := getBlobStoreNameByRepoType(repoType)

	// Ensure the blob store exists
	log.Printf("Ensuring blob store '%s' exists...\n", blobStoreName)
	if err := createOrEnsureBlobStore(targetBaseURL, blobStoreName, targetUser, targetPass); err != nil {
		return fmt.Errorf("failed to ensure blob store: %w", err)
	}

	// Adjust repository name and blob store
	srcDef["name"] = targetRepo
	if storage, ok := srcDef["storage"].(map[string]interface{}); ok {
		storage["blobStoreName"] = blobStoreName
	}

	// Create the repository on the target
	log.Printf("Ensuring repository '%s' exists on target...\n", targetRepo)
	if err := createRepoOnTarget(targetBaseURL, srcDef, targetUser, targetPass); err != nil {
		return fmt.Errorf("failed to create repository on target: %w", err)
	}

	// Gracefully exit if the repository type is 'group' or 'proxy'
	if repoType == "group" || repoType == "proxy" {
		log.Printf("Source repository '%s' is of type '%s'. Exiting gracefully after creating the target repository and blob store.\n", sourceRepo, repoType)
		os.Exit(0)
	}

	return nil
}

func getRepositoryDefinition(baseURL, repoName, user, pass string) (map[string]interface{}, error) {
	endpoint := fmt.Sprintf("%s/service/rest/v1/repositories/%s", strings.TrimRight(baseURL, "/"), url.PathEscape(repoName))

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("creating GET request failed: %w", err)
	}
	if user != "" && pass != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET repository definition error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to fetch repository definition: status %d, body: %s", resp.StatusCode, string(body))
	}

	var def map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&def); err != nil {
		return nil, fmt.Errorf("decoding repository definition JSON: %w", err)
	}

	return def, nil
}

func createRepoOnTarget(baseURL string, repoDef map[string]interface{}, user, pass string) error {
	cleanRepoDefinition(repoDef)

	endpoint := fmt.Sprintf("%s/service/rest/v1/repositories", strings.TrimRight(baseURL, "/"))
	payload, err := json.Marshal(repoDef)
	if err != nil {
		return fmt.Errorf("failed to marshal repository definition: %w", err)
	}

	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("creating POST request failed: %w", err)
	}
	if user != "" && pass != "" {
		req.SetBasicAuth(user, pass)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST repository creation error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create repository: status %d, body: %s", resp.StatusCode, string(body))
	}

	log.Printf("Repository '%s' successfully created on target.\n", repoDef["name"])
	return nil
}

// ----------------------------------------------------------------------
// getBlobStoreNameByRepoType: Determines the blob store name based on repo type
// ----------------------------------------------------------------------
func getBlobStoreNameByRepoType(repoType string) string {
	switch repoType {
	case "maven2":
		return "maven"
	default:
		return repoType
	}
}

// ----------------------------------------------------------------------
// cleanRepoDefinition: Cleans up repository definition for POST
// ----------------------------------------------------------------------
func cleanRepoDefinition(def map[string]interface{}) {
	delete(def, "_links")
	delete(def, "url")
	delete(def, "format")
	delete(def, "type")
	delete(def, "contentPermissions")
}

// ----------------------------------------------------------------------
// getGroupsForRepo checks if there is a manually specified list of groups
// for the given repo. If yes, it returns that list. If not, we auto-discover
// them via listAllGroupsFromSource.
// ----------------------------------------------------------------------
func getGroupsForRepo(baseURL, repo, user, pass string) ([]string, error) {
    if groups, ok := manualRepoGroups[repo]; ok {
        // We have a manual override
        log.Printf("Using manually specified groups for repo '%s': %v\n", repo, groups)
        return groups, nil
    }

    // Otherwise, discover groups automatically
    log.Printf("No manual group override for repo '%s'. Auto-discovering groups...\n", repo)
    return listAllGroupsFromSource(baseURL, repo, user, pass)
}

// ----------------------------------------------------------------------
// listAllGroupsFromSource: enumerates all unique groups in the given repo
// by paging through /service/rest/v1/search?repository=...
// ----------------------------------------------------------------------
func listAllGroupsFromSource(baseURL, repo, user, pass string) ([]string, error) {
    var allGroups []string
    continuation := ""

    for {
        reqURL := fmt.Sprintf("%s/service/rest/v1/search?repository=%s",
            strings.TrimRight(baseURL, "/"),
            url.PathEscape(repo),
        )
        if continuation != "" {
            reqURL += "&continuationToken=" + url.QueryEscape(continuation)
        }

        req, err := http.NewRequest("GET", reqURL, nil)
        if err != nil {
            return nil, err
        }
        if user != "" && pass != "" {
            req.SetBasicAuth(user, pass)
        }

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        if resp.StatusCode != http.StatusOK {
            resp.Body.Close()
            return nil, fmt.Errorf("listAllGroupsFromSource: status code %d", resp.StatusCode)
        }

        var sr NexusSearchResponse
        if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
            resp.Body.Close()
            return nil, err
        }
        resp.Body.Close()

        // Collect groups from this page
        for _, item := range sr.Items {
            if !contains(allGroups, item.Group) {
                allGroups = append(allGroups, item.Group)
            }
        }

        if sr.ContinuationToken == "" {
            break // no more pages
        }
        continuation = sr.ContinuationToken
    }

    return allGroups, nil
}

// ----------------------------------------------------------------------
// queryArtifactsByGroup: now loop until continuationToken is empty, so
// that you don't miss any items if a single group has more than one page.
// ----------------------------------------------------------------------
func queryArtifactsByGroup(group string) ([]NexusSearchItem, error) {
    var allItems []NexusSearchItem
    continuation := ""

    for {
        srcSearchURL := fmt.Sprintf("%s/service/rest/v1/search?repository=%s&group=%s",
            strings.TrimRight(*sourceURL, "/"),
            url.QueryEscape(*sourceRepo),
            url.QueryEscape(group),
        )
        if continuation != "" {
            srcSearchURL += "&continuationToken=" + url.QueryEscape(continuation)
        }

        req, err := http.NewRequest("GET", srcSearchURL, nil)
        if err != nil {
            return nil, err
        }
        if *sourceUser != "" && *sourcePass != "" {
            req.SetBasicAuth(*sourceUser, *sourcePass)
        }

        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            return nil, fmt.Errorf("queryArtifactsByGroup: unexpected status code %d", resp.StatusCode)
        }

        var sr NexusSearchResponse
        if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
            return nil, err
        }

        allItems = append(allItems, sr.Items...)

        if sr.ContinuationToken == "" {
            break
        }
        continuation = sr.ContinuationToken
    }

    return allItems, nil
}

// ----------------------------------------------------------------------
// processArtifact: loops over *all* item.Assets. Each asset is checked
// for existence, then (if missing) is transferred with a streaming approach.
// ----------------------------------------------------------------------
func processArtifact(item NexusSearchItem) error {
    if len(item.Assets) == 0 {
        return fmt.Errorf("no assets for item %s:%s", item.Group, item.Name)
    }

    var lastErr error
    for _, asset := range item.Assets {
        err := processSingleAsset(item, asset)
        if err != nil {
            // We log the error but keep going for other assets
            log.Printf("[ERROR in processSingleAsset] %v\n", err)
            lastErr = err
        }
    }
    return lastErr
}

// ----------------------------------------------------------------------
// processSingleAsset: checks existence on target, then streams the file
// from source to target if missing.
// ----------------------------------------------------------------------
func processSingleAsset(item NexusSearchItem, asset NexusAsset) error {
    exists, err := artifactExistsOnTarget(item, asset)
    if err != nil {
        return fmt.Errorf("checking existence on target (asset %s): %w", asset.Path, err)
    }
    if exists {
        log.Printf("[SKIP] Already exists on target: %s:%s (%s)",
            item.Group, item.Name, asset.Path)
        return nil
    }

    // Stream from source to target using io.Pipe()
    if err := transferAssetViaPipe(asset); err != nil {
        return fmt.Errorf("streaming asset %s:%s => %w", item.Group, item.Name, err)
    }

    log.Printf("[OK] Transferred: %s:%s (%s)", item.Group, item.Name, asset.Path)
    return nil
}

// ----------------------------------------------------------------------
// artifactExistsOnTarget:
// 1) If we have a recognized checksum in the source asset (sha256, sha1, md5),
//    do a direct search by hash on the target. If found => exists
// 2) Otherwise (or if not found), fallback to group/name/version + path check
// ----------------------------------------------------------------------
func artifactExistsOnTarget(item NexusSearchItem, asset NexusAsset) (bool, error) {
    hashType, hashValue := pickPreferredHash(asset)
    if hashType != "" && hashValue != "" {
        // Attempt direct search by hash
        found, err := searchByHashOnTarget(hashType, hashValue)
        if err != nil {
            return false, fmt.Errorf("searchByHashOnTarget(%s) failed: %w", hashType, err)
        }
        if found {
            return true, nil
        }
    }

    // Fallback if no recognized hash or not found by hash
    return fallbackPathCheck(item, asset)
}

// ----------------------------------------------------------------------
// pickPreferredHash: tries sha256, then sha1, then md5
// ----------------------------------------------------------------------
func pickPreferredHash(asset NexusAsset) (string, string) {
    if val, ok := asset.Checksum["sha256"]; ok && val != "" {
        return "sha256", val
    }
    if val, ok := asset.Checksum["sha1"]; ok && val != "" {
        return "sha1", val
    }
    if val, ok := asset.Checksum["md5"]; ok && val != "" {
        return "md5", val
    }
    return "", ""
}

// ----------------------------------------------------------------------
// searchByHashOnTarget: pages through /service/rest/v1/search?sha1=xxx (or sha256=..., md5=...)
// If any item is found, we conclude the artifact exists
// ----------------------------------------------------------------------
func searchByHashOnTarget(hashType, hashValue string) (bool, error) {
    cont := ""
    for {
        // e.g.:  /service/rest/v1/search?repository=myrepo&sha256=abc123
        queryKey := url.QueryEscape(hashType) // "sha256", "sha1", or "md5"
        queryURL := fmt.Sprintf(
            "%s/service/rest/v1/search?repository=%s&%s=%s",
            strings.TrimRight(*targetURL, "/"),
            url.QueryEscape(*targetRepo),
            queryKey,
            url.QueryEscape(hashValue),
        )
        if cont != "" {
            queryURL += "&continuationToken=" + url.QueryEscape(cont)
        }

        req, err := http.NewRequest("GET", queryURL, nil)
        if err != nil {
            return false, err
        }
        if *targetUser != "" && *targetPass != "" {
            req.SetBasicAuth(*targetUser, *targetPass)
        }

        resp, err := client.Do(req)
        if err != nil {
            return false, err
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            return false, fmt.Errorf("searchByHashOnTarget: code %d", resp.StatusCode)
        }

        var sr NexusSearchResponse
        if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
            return false, err
        }

        if len(sr.Items) > 0 {
            // Found at least one item => artifact with this hash exists
            return true, nil
        }
        if sr.ContinuationToken == "" {
            break
        }
        cont = sr.ContinuationToken
    }
    return false, nil
}

// ----------------------------------------------------------------------
// fallbackPathCheck: group/name/version search, then compare asset.Path
// ----------------------------------------------------------------------
func fallbackPathCheck(item NexusSearchItem, asset NexusAsset) (bool, error) {
    cont := ""
    for {
        queryURL := fmt.Sprintf("%s/service/rest/v1/search?repository=%s&group=%s&name=%s&version=%s",
            strings.TrimRight(*targetURL, "/"),
            url.QueryEscape(*targetRepo),
            url.QueryEscape(item.Group),
            url.QueryEscape(item.Name),
            url.QueryEscape(item.Version),
        )
        if cont != "" {
            queryURL += "&continuationToken=" + url.QueryEscape(cont)
        }

        req, err := http.NewRequest("GET", queryURL, nil)
        if err != nil {
            return false, err
        }
        if *targetUser != "" && *targetPass != "" {
            req.SetBasicAuth(*targetUser, *targetPass)
        }

        resp, err := client.Do(req)
        if err != nil {
            return false, err
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            return false, fmt.Errorf("fallbackPathCheck: code %d", resp.StatusCode)
        }

        var sr NexusSearchResponse
        if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
            return false, err
        }

        // Compare path
        // Normalize paths
        normalizedAssetPath := strings.TrimPrefix(asset.Path, "/")
        for _, fItem := range sr.Items {
            for _, fAsset := range fItem.Assets {
                normalizedFAssetPath := strings.TrimPrefix(fAsset.Path, "/")
                if normalizedFAssetPath == normalizedAssetPath {
                    return true, nil
                }
            }
        }

        if sr.ContinuationToken == "" {
            break
        }
        cont = sr.ContinuationToken
    }
    return false, nil
}

// ----------------------------------------------------------------------
// transferAssetViaPipe: Streams data from the source (asset.DownloadURL)
// to the target (PUT /repository/<repo>/<asset.Path>) via io.Pipe().
// This avoids reading the entire file into memory.
// ----------------------------------------------------------------------
func transferAssetViaPipe(asset NexusAsset) error {
    // 1) Build a GET request for the source
    srcReq, err := http.NewRequest("GET", asset.DownloadURL, nil)
    if err != nil {
        return fmt.Errorf("creating GET request for source: %v", err)
    }
    // Basic Auth for source if needed
    if *sourceUser != "" && *sourcePass != "" {
        srcReq.SetBasicAuth(*sourceUser, *sourcePass)
    }

    // 2) Perform the GET to the source
    srcResp, err := client.Do(srcReq)
    if err != nil {
        return fmt.Errorf("GET source error: %v", err)
    }
    if srcResp.StatusCode != http.StatusOK {
        srcResp.Body.Close()
        return fmt.Errorf("source returned status %d for %s", srcResp.StatusCode, asset.DownloadURL)
    }

    // 3) Create an io.Pipe()
    r, w := io.Pipe()

    // In a separate goroutine, copy from srcResp.Body => pipe's writer
    go func() {
        defer w.Close()
        _, copyErr := io.Copy(w, srcResp.Body)
        srcResp.Body.Close()
        if copyErr != nil {
            // If copying fails, propagate error to the reader side
            w.CloseWithError(copyErr)
        }
    }()

    // 4) Prepare a PUT request to the target
    putURL := fmt.Sprintf("%s/repository/%s/%s",
        strings.TrimRight(*targetURL, "/"),
        url.PathEscape(*targetRepo),
        url.PathEscape(asset.Path),
    )

    putReq, err := http.NewRequest("PUT", putURL, r)
    if err != nil {
        return fmt.Errorf("creating PUT request: %v", err)
    }
    // Basic Auth for target if needed
    if *targetUser != "" && *targetPass != "" {
        putReq.SetBasicAuth(*targetUser, *targetPass)
    }
    putReq.Header.Set("Content-Type", "application/octet-stream")

    // 5) Execute the PUT, reading from the pipe
    putResp, err := client.Do(putReq)
    if err != nil {
        return fmt.Errorf("PUT target error: %v", err)
    }
    defer putResp.Body.Close()

    if putResp.StatusCode < 200 || putResp.StatusCode > 299 {
        bodyBytes, _ := io.ReadAll(putResp.Body)
        return fmt.Errorf("upload to target failed, code %d: %s",
            putResp.StatusCode, string(bodyBytes))
    }

    return nil
}

// ----------------------------------------------------------------------
// Utility functions for environment variables and slice checks
// ----------------------------------------------------------------------
func getEnv(key, defaultValue string) string {
    if value, exists := os.LookupEnv(key); exists {
        return value
    }
    return defaultValue
}

func getEnvAsInt(name string, defaultVal int) int {
    if valueStr, exists := os.LookupEnv(name); exists {
        if value, err := strconv.Atoi(valueStr); err == nil {
            return value
        }
    }
    return defaultVal
}

func contains(slice []string, item string) bool {
    for _, s := range slice {
        if s == item {
            return true
        }
    }
    return false
}
