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
    Checksum    map[string]string `json:"checksum"`
}

type NexusSearchResponse struct {
    Items             []NexusSearchItem `json:"items"`
    ContinuationToken string            `json:"continuationToken"`
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

    client = &http.Client{
        Timeout: 120 * time.Second, // shared HTTP client with global timeout
    }
)

func main() {
    flag.Parse()

    // Validate required flags
    if *sourceURL == "" || *sourceRepo == "" || *targetURL == "" || *targetRepo == "" {
        log.Fatal("Error: Required flags are not provided. --source-url, --source-repo, --target-url, --target-repo are mandatory.")
    }

    log.Printf("Starting Nexus transfer\n")
    log.Printf("Source: %s (repo: %s)\n", *sourceURL, *sourceRepo)
    log.Printf("Target: %s (repo: %s)\n", *targetURL, *targetRepo)

    // ----------------------------------------------------------------------
    // 1. Get the effective groups for this repo, either manually specified
    //    or discovered automatically
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
                    log.Printf("[ERROR] %v\n", err)
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
// getGroupsForRepo checks if there is a manually specified list of groups
// for the given repo. If yes, it returns that list. If not, it calls
// listAllGroupsFromSource to auto-discover them.
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
            reqURL = fmt.Sprintf("%s&continuationToken=%s", reqURL, url.QueryEscape(continuation))
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
// processArtifact: checks if item already exists on target, downloads
// if missing, then uploads to target repo.
// ----------------------------------------------------------------------
func processArtifact(item NexusSearchItem) error {
    if len(item.Assets) == 0 {
        return fmt.Errorf("no assets for item %s:%s", item.Group, item.Name)
    }
    asset := item.Assets[0]

    exists, err := artifactExistsOnTarget(item)
    if err != nil {
        return fmt.Errorf("checking existence on target: %w", err)
    }
    if exists {
        log.Printf("[SKIP] Already exists on target: %s:%s (%s)",
            item.Group, item.Name, asset.Path)
        return nil
    }

    artifactData, err := downloadAsset(asset.DownloadURL)
    if err != nil {
        return fmt.Errorf("downloading asset: %s:%s => %w",
            item.Group, item.Name, err)
    }

    if err := uploadToTarget(asset, artifactData); err != nil {
        return fmt.Errorf("uploading to target: %s:%s => %w",
            item.Group, item.Name, err)
    }

    log.Printf("[OK] Transferred: %s:%s (%s)",
        item.Group, item.Name, asset.Path)
    return nil
}

// ----------------------------------------------------------------------
// artifactExistsOnTarget: checks if there's already a group/name match
// on the target. (This version does not check version, but you could
// extend it to do so by adding &version=%s.)
// ----------------------------------------------------------------------
func artifactExistsOnTarget(item NexusSearchItem) (bool, error) {
    tgtSearchURL := fmt.Sprintf("%s/service/rest/v1/search?repository=%s&group=%s&name=%s",
        strings.TrimRight(*targetURL, "/"),
        url.QueryEscape(*targetRepo),
        url.QueryEscape(item.Group),
        url.QueryEscape(item.Name),
    )

    req, err := http.NewRequest("GET", tgtSearchURL, nil)
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
        return false, fmt.Errorf("artifactExistsOnTarget: unexpected status code %d", resp.StatusCode)
    }

    var sr NexusSearchResponse
    if err := json.NewDecoder(resp.Body).Decode(&sr); err != nil {
        return false, err
    }

    return len(sr.Items) > 0, nil
}

// ----------------------------------------------------------------------
// downloadAsset: simple GET to fetch the artifact from source
// ----------------------------------------------------------------------
func downloadAsset(downloadURL string) ([]byte, error) {
    req, err := http.NewRequest("GET", downloadURL, nil)
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
        return nil, fmt.Errorf("downloadAsset: unexpected status code %d", resp.StatusCode)
    }

    return io.ReadAll(resp.Body)
}

// ----------------------------------------------------------------------
// uploadToTarget: PUT the artifact bytes to the target (raw-style).
// ----------------------------------------------------------------------
func uploadToTarget(asset NexusAsset, data []byte) error {
    putURL := fmt.Sprintf("%s/repository/%s/%s",
        strings.TrimRight(*targetURL, "/"),
        url.PathEscape(*targetRepo),
        url.PathEscape(asset.Path),
    )

    req, err := http.NewRequest("PUT", putURL, bytes.NewReader(data))
    if err != nil {
        return err
    }

    if *targetUser != "" && *targetPass != "" {
        req.SetBasicAuth(*targetUser, *targetPass)
    }

    req.Header.Set("Content-Type", "application/octet-stream")

    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode < 200 || resp.StatusCode > 299 {
        return fmt.Errorf("uploadToTarget: unexpected status code %d", resp.StatusCode)
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
