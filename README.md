# Monitor-Blobstorages.ps1

## Purpose
This script is an Azure Automation style monitoring runbook that inventories Azure Storage Accounts across subscriptions and sends consolidated storage telemetry into Log Analytics through a Data Collection Rule (DCR) and Data Collection Endpoint (DCE).

## Review Status
Reviewed against the current script implementation in April 2026.

Overall assessment:
- Functional and feature-rich for broad storage posture inventory.
- Works as a best-effort collector, but has several reliability/performance issues that should be addressed for production robustness at scale.

## Core Functionality
- Authenticates to Azure using managed identity.
- Discovers all Storage Accounts using Azure Resource Graph.
- Collects account-level configuration and usage signals, including:
	- Used capacity and transaction metrics.
	- Blob retention, versioning, and anonymous access settings.
	- Static website endpoint status.
	- Azure Files usage.
	- SFTP enablement, TLS minimum version, firewall mode, HNS status.
	- Defender for Storage status.
	- Network Security Perimeter linkage.
	- Backup protection status (queried from Log Analytics).
	- Daily cost data via Azure Cost Management API (original billing currency plus USD and EUR values).
- Normalizes data types and schema-aligned property names.
- Posts batched records to a custom Log Analytics table.

## Main Execution Flow
1. Imports Az modules and signs in with managed identity.
2. Reads required secrets from Key Vault (ingestion app secret and read-only app credentials).
3. Builds ARM REST headers from managed identity token.
4. Retrieves storage account inventory and extended metadata from Resource Graph.
5. Iterates each subscription and storage account to gather metrics and settings.
6. Calls helper functions/APIs for backup status, cost, Defender, and NSP details.
7. Builds a flattened object list for ingestion.
8. Runs schema-alignment helper functions.
9. Sends data to Log Analytics using DCR/DCE ingestion.

## Review Findings (Highest to Lowest Severity)
1. Backup query is executed per storage account (high performance impact).
	- Current behavior: `Get-StorageAccountsInBackupConfig` is called inside the per-account loop.
	- Impact: Repeated OAuth token requests and repeated Log Analytics queries create avoidable latency and API pressure.
	- Recommendation: Execute once per run (or per subscription if needed), cache in a hash set, and only perform lookups in memory.

2. Metric time values are passed as strings (`"HH:mm:ss"`) instead of DateTime.
	- Current behavior: `Get-AzMetric` calls use string-formatted times.
	- Impact: Ambiguous parsing, potential incorrect windows, and inconsistent results.
	- Recommendation: Pass DateTime objects directly (for example `$startTime = (Get-Date).AddHours(-2)`), not formatted strings.

3. Global `$ErrorActionPreference = "SilentlyContinue"` hides operational failures.
	- Current behavior: Errors are suppressed globally, while some calls have local `try/catch`.
	- Impact: Partial failures can be missed, making troubleshooting hard.
	- Recommendation: Prefer `$ErrorActionPreference = "Stop"` and selectively suppress non-critical calls with explicit `-ErrorAction`.

4. Cost handling mixes average/sum semantics and converts numeric values to strings early.
	- Current behavior: Per-day costs may be divided by `DaysBack`, then later summed, then converted to strings.
	- Impact: Field names like `DailyCostSum*` can be misleading; numeric precision and downstream query behavior may degrade.
	- Recommendation: Keep numeric types through ingestion and separate explicit fields (for example `DailyCostAverage*` vs `DailyCostTotal*`).

5. NSP handling only keeps the last linked perimeter when multiple exist.
	- Current behavior: Foreach over NSP entries overwrites output fields.
	- Impact: Incomplete reporting for accounts linked to multiple perimeters.
	- Recommendation: Join names/states into delimited strings or emit multiple records/array fields.

6. Function-level `exit 1` inside helper function can terminate the run unexpectedly.
	- Current behavior: `Get-StorageAccountsInBackupConfig` uses `exit 1` on token failure.
	- Impact: One dependency failure can stop all collection.
	- Recommendation: Return an empty array and let caller decide behavior/logging.

7. Duplicate account retrieval in loop (`Get-AzStorageAccount` called multiple times).
	- Current behavior: Account object is fetched more than once per storage account.
	- Impact: Extra ARM calls and runtime overhead.
	- Recommendation: Retrieve once and reuse.

## Azure Modules Leveraged
Explicitly imported in script:
- Az.Accounts
	- Authentication, subscription context, token context.
- Az.ResourceGraph
	- Fast cross-subscription storage account discovery and property projection.
- Az.KeyVault
	- Secure retrieval of credentials and secrets.
- Az.Storage
	- Storage account settings, blob service properties, and file share checks.
- AzDcrLogIngest
	- DCR/DCE helper cmdlets for table schema alignment and custom log ingestion.

Implicitly required (cmdlet autoload in many Automation environments):
- Az.Monitor
	- Required for `Get-AzMetric` used to collect UsedCapacity and Transactions metrics.

Notes:
- If module autoload is disabled or restricted in the Automation account, add an explicit `Import-Module -Name Az.Monitor`.
- The script currently imports 5 modules directly: `Az.Accounts`, `Az.ResourceGraph`, `Az.KeyVault`, `Az.Storage`, `AzDcrLogIngest`.

## Azure APIs Leveraged (REST)
- Managed Identity endpoint
	- Gets ARM token for direct management API calls.
- Azure Cost Management Query API
	- Retrieves daily cost rows for each storage account.
- Microsoft.Security advancedThreatProtectionSettings
	- Reads Defender for Storage enablement.
- Storage networkSecurityPerimeterConfigurations API
	- Retrieves NSP association and provisioning state.
- Log Analytics Query API
	- Detects backup-protected storage accounts.

## Key Custom Functions in the Script
- Get-ManagedIdentityArmHeaders
	- Builds ARM authorization headers via managed identity.
- Get-StorageAccountDailyCost
	- Queries cost per storage account and converts currency totals.
- Get-StorageAccountsInBackupConfig
	- Queries Log Analytics for backup-protected storage accounts.
- Get-AzStorageAccountList
	- Returns all storage accounts from Resource Graph with pagination.
- Get-AzStorageAccountExtendedProperties
	- Returns firewall, TLS, location, and HNS fields from Resource Graph.
- Get-AzStorageAccountNetworkSecurityPerimeterInfo
	- Returns linked NSP names and provisioning state.

## Ingestion and Table Output
The script builds one record per storage account and submits records in batches (configured as 500) into the configured custom table name (blobmonitor_CL naming pattern via DCR convention).

Current output fields include:
- Identity and posture: `StorageAccount`, `Location`, `FirewallSettings`, `MinimumTlsVersion`, `HierarchicalNamespaceEnabled`.
- Data protection: `IsRetentionEnabled`, `RetentionDays`, `IsVersioningEnabled`, `VersioningDays`, `IsConfiguredForBackup`.
- Workload/config: `IsAzureFilesUsed`, `AzureFileshareCount`, `HasStaticWebsiteEndPoint`, `IsSFTPEnabled`.
- Security: `IsAnonymousAccessEnabled`, `IsDefenderEnabled`, `NetworkSecurityPerimeterName`, `NetworkSecurityPerimeterProvisioningState`.
- Usage and cost: `SizeInBytes`, `Transactions`, `BillingCurrency`, `DailyCostAvgOriginal`, `DailyCostAvgUSD`, `DailyCostAvgEUR`.

## Operational Notes
- Global error behavior is set to SilentlyContinue, while many critical calls are wrapped in local try/catch blocks.
- Several values are defaulted (for example to 0, False, Unknown, or 0.0001) when APIs return empty data.
- Cost data can lag due to Cost Management reporting latency.
- The script expects additional helper functions for DCR/DCE operations to be available in the Automation environment:
	- Get-AzDceListAll
	- Get-AzDcrListAll
	- ValidateFix-AzLogAnalyticsTableSchemaColumnNames
	- Build-DataArrayToAlignWithSchema
	- Post-AzLogAnalyticsLogIngestCustomLogDcrDce-Output

