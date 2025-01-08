# Nexus Migration Tool

This project provides a tool for migrating artifacts between Nexus repositories. It allows users to transfer artifacts from a source Nexus server to a target Nexus server, with configurable options for repositories and authentication.

## Project Structure

- `src/nexus-sync.go`: Contains the main logic for migrating artifacts, including the definition of the `NexusSearchResponse` struct, manual mapping of repositories to groups, and command-line flags for configuration.
- `go.mod`: The Go module definition file that specifies the module's name and its dependencies.

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd nexus-migrate
   ```

2. **Install dependencies:**
   ```
   go mod tidy
   ```

3. **Configure environment variables:**
   Set the following environment variables to configure the source and target Nexus servers:
   - `SOURCE_URL`: Base URL of the source Nexus server
   - `SOURCE_USER`: Username for the source Nexus server
   - `SOURCE_PASS`: Password for the source Nexus server
   - `SOURCE_REPO`: Repository name on the source server
   - `TARGET_URL`: Base URL of the target Nexus server
   - `TARGET_USER`: Username for the target Nexus server
   - `TARGET_PASS`: Password for the target Nexus server
   - `TARGET_REPO`: Repository name on the target server
   - `CONCURRENCY`: Number of concurrent workers for transfer (optional, default is 8)

## Usage

Run the migration tool with the following command:
```
go run src/nexus-sync.go --source-url <source-url> --source-repo <source-repo> --target-url <target-url> --target-repo <target-repo>
```

Ensure that all required flags are provided. The tool will log the starting parameters and begin the transfer process.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.