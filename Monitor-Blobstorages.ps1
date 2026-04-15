<#
.SYNOPSIS
Monitors Azure Blob Storage accounts and collects configuration and usage metrics.

.DESCRIPTION
This runbook connects to Azure using managed identity and retrieves comprehensive
information about all storage accounts across subscriptions, including blob service
properties, backup configurations, and security settings. Data is collected and sent
to Log Analytics via Data Collection Rule (DCR) and Data Collection Endpoint (DCE).

.PARAMETER None
This runbook uses Azure Automation variables and Key Vault secrets for configuration.

.NOTES
Prerequisites:
- Azure Automation account with managed identity enabled
- Key Vault with secrets for app authentication and credentials
- Log Analytics workspace and DCR/DCE configured
- Required Azure modules: Az.Accounts, Az.ResourceGraph, Az.KeyVault, Az.Storage

Configuration Variables:
- $azIOKeyVaultName: Key Vault containing credentials
- $LogIngestAppId: Application ID for Log Analytics ingestion
- $LogAnalyticsWorkspaceResourceId: Target Log Analytics workspace resource ID
- $AzDceName: Data Collection Endpoint name
- $AzDcrName: Data Collection Rule name
- $TableName: Log Analytics table name for storage data

Collected Data:
- Storage account names and locations
- Blob usage metrics (size, transaction count)
- Security settings (anonymous access, TLS version, firewall rules)
- Data protection features (retention, versioning, backup status)
- Azure Files and SFTP configuration
- Redundancy and access tier information
- Microsoft Defender for Storage status

Error Handling:
- Silent continue on errors to allow script continuation
- Retry logic for metrics collection with extended time ranges
- Graceful fallback for unavailable data
- Detailed error logging for troubleshooting

Output:
Data is posted to Log Analytics workspace via DCR/DCE in batches of 500 records.
#>


$erroractionpreference = "SilentlyContinue"

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

Import-Module -Name Az.Accounts 
Import-Module -Name Az.ResourceGraph
Import-Module -Name Az.KeyVault
Import-Module -Name Az.Storage
Import-Module -Name AzDcrLogIngest

Connect-AzAccount -Identity

$verbose = $true 

$VerbosePreference                          = "Continue"

#region customize these variables for your environment

$tenantId                                   = YOURTENANTID
$azIOKeyVaultName                           = KEYVAULTNAME
$azIOKeyName                                = NAMEOFTHEKEYFORLOGINGESTION
$azKVLogIngestKey                           = Get-AzKeyVaultSecret -VaultName $azIOKeyVaultName -Name $azIOKeyName
$azKVLogIngestVal                           = $azKVLogIngestKey.SecretValue
$LogIngestAppSecret                         = $azKVLogIngestVal | ConvertFrom-SecureString -AsPlainText
$LogIngestAppId                             = CLIENTIDFORLOGINGESTIONAPPLICATION_CREATED_FOR_AZDCRLOGINGEST

$LogAnalyticsWorkspaceResourceId            = "/subscriptions/ID/resourceGroups/RSGNAME/providers/Microsoft.OperationalInsights/workspaces/LOGANALYTICSWORKSPACENAME"

$AzDcrResourceGroup                         = "rsg-YOUR_RSG_NAME-dcr"
$AzDcrPrefix                                = "prd"

$AzDceName                                  = "dce-YOUR_DCE_NAME"
$TableName                                  = "blobmonitor"
$AzDcrName                                  = "dcr-prd-" + $TableName + "_CL"                       

$PowershellReadAccessClientIDKey            = Get-AzKeyVaultSecret -VaultName $azIOKeyVaultName -Name "PowershellReadAccessClientID"
$PowershellReadAccessClientIDVal            = $PowershellReadAccessClientIDKey.SecretValue | ConvertFrom-SecureString -AsPlainText

$PowershellReadAccessClientSecretKey        = Get-AzKeyVaultSecret -VaultName $azIOKeyVaultName -Name "PowershellReadAccessClientSecret"
$PowershellReadAccessClientSecretVal        = $PowershellReadAccessClientSecretKey.SecretValue | ConvertFrom-SecureString -AsPlainText

#endregion

$AzDcrSetLogIngestApiAppPermissionsDcrLevel = $false    
$AzLogDcrTableCreateFromReferenceMachine    = @()
$AzLogDcrTableCreateFromAnyMachine          = $true


$apiVersion = '2019-08-01' 


function Get-ManagedIdentityArmHeaders {
    <#
    .SYNOPSIS
    Retrieves Azure Resource Manager headers using the automation account's managed identity.

    .DESCRIPTION
    Requests an access token from the managed identity endpoint for the ARM resource and
    returns the authorization headers needed for subsequent REST calls. Writes a short
    success message when a token is acquired.

    .PARAMETER ApiVersion
    API version used when calling the managed identity endpoint. Defaults to 2019-08-01.

    .PARAMETER Resource
    Resource URI for which to request a token. Defaults to https://management.azure.com/.

    .OUTPUTS
    Hashtable containing Authorization and Content-Type headers for ARM REST requests.
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [string]$ApiVersion = '2019-08-01',
        [Parameter(Mandatory = $false)]
        [string]$Resource = 'https://management.azure.com/'
    )

    try {
        $initialHeaders = @{}
        if ($env:IDENTITY_HEADER) {
            $initialHeaders["X-IDENTITY-HEADER"] = $env:IDENTITY_HEADER
        }

        $url = "$($env:IDENTITY_ENDPOINT)?api-version=$ApiVersion&resource=$Resource"
        $response = Invoke-RestMethod -Method Get -Uri $url -Headers $initialHeaders -ErrorAction Stop
        $accessToken = $response.access_token

        Write-Verbose "Successfully obtained access token $($accessToken.Substring(0, 10))..."

        return @{
            Authorization = "Bearer $accessToken"
            "Content-Type" = "application/json"
        }
    } catch {
        Write-Error "Failed to obtain access token from managed identity: $_ ; terminating script"
        return $null
    }
} #end function Get-ManagedIdentityArmHeaders

$headers = Get-ManagedIdentityArmHeaders -ApiVersion $apiVersion


function Get-StorageAccountDailyCost {
    <#
    .SYNOPSIS
    Retrieves the daily cost for a specific storage account in USD and EUR.

    .DESCRIPTION
    Queries the Cost Management API to retrieve actual daily costs for a given storage
    account within a specified date range. Uses managed identity authentication and returns
    costs in both USD and EUR by determining the subscription's billing currency from the API
    and applying appropriate currency conversion if needed.

    .PARAMETER StorageAccountResourceId
    The full Azure resource ID of the storage account (e.g., /subscriptions/xxx/resourceGroups/yyy/providers/Microsoft.Storage/storageAccounts/zzz).

    .PARAMETER SubscriptionId
    The subscription ID containing the storage account.

    .PARAMETER Headers
    The authorization headers (hashtable) obtained from Get-ManagedIdentityArmHeaders.

    .PARAMETER DaysBack
    Number of days to look back from today. Defaults to 7.

    .OUTPUTS
    PSCustomObject with properties: CostDate, BillingCurrency, PreTaxCostOriginal, PreTaxCostUSD, PreTaxCostEUR. Returns $null if query fails.

    .EXAMPLE
    $cost = Get-StorageAccountDailyCost -StorageAccountResourceId "/subscriptions/.../storageAccounts/myaccount" -SubscriptionId "xxx" -Headers $headers
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$StorageAccountResourceId,
        [Parameter(Mandatory = $true)]
        [string]$SubscriptionId,
        [Parameter(Mandatory = $true)]
        [hashtable]$Headers,
        [Parameter(Mandatory = $false)]
        [int]$DaysBack = 7
    )

    try {
        # Exchange rates for currency conversion (can be updated to use real-time API if needed)
        $exchangeRates = @{
            'USD' = @{ 'USD' = 1.0;  'EUR' = 0.92 }
            'EUR' = @{ 'USD' = 1.09; 'EUR' = 1.0 }            
        }

        $endDate = Get-Date
        $startDate = $endDate.AddDays(-$DaysBack)

        $body = @{
            type       = "ActualCost"
            timeframe  = "Custom"
            timePeriod = @{
                from = $startDate.ToString("yyyy-MM-dd")
                to   = $endDate.ToString("yyyy-MM-dd")
            }
            dataset    = @{
                granularity = "Daily"
                filter      = @{
                    dimensions = @{
                        name     = "ResourceId"
                        operator = "In"
                        values   = @($StorageAccountResourceId.ToLower())
                    }
                }
                aggregation = @{
                    totalCost = @{
                        name     = "PreTaxCost"
                        function = "Sum"
                    }
                }
            }
        } | ConvertTo-Json -Depth 6

        $scope = "/subscriptions/$SubscriptionId"
        $uri = "https://management.azure.com$scope/providers/Microsoft.CostManagement/query?api-version=2023-03-01"

        $response = Invoke-RestMethod -Uri $uri -Method Post -Headers $Headers -Body $body -ContentType "application/json" -ErrorAction Stop

        # Determine billing currency from API response
        $billingCurrency = if ($response.properties.billingPeriodIds -and $response.properties.billingPeriodIds[0]) {
            # Extract currency from response or default to USD
            if ($response.properties -and $response.properties.currency) {
                $response.properties.currency
            } else {
                "USD"  # Default to USD if currency cannot be determined
            }
        } else {
            "USD"
        }

        Write-Verbose "Billing currency determined: $billingCurrency"

        if ($response.properties.rows) {
            $columns = @{}
            for ($i = 0; $i -lt $response.properties.columns.Count; $i++) {
                $col = $response.properties.columns[$i]
                $columns[$col.name] = $i
            }

            $dateIndex = if ($columns.ContainsKey('UsageDate')) { $columns['UsageDate'] } elseif ($columns.ContainsKey('BillingDate')) { $columns['BillingDate'] } else { 0 }
            $costIndex = if ($columns.ContainsKey('PreTaxCost')) { $columns['PreTaxCost'] } elseif ($columns.ContainsKey('Cost')) { $columns['Cost'] } else { 1 }

            Write-Verbose "Cost column index: $costIndex, Date column index: $dateIndex; columns: $($columns.Keys -join ','); BillingCurrency: $billingCurrency"

            $costData = @()
            foreach ($row in $response.properties.rows) {
                $originalCost = [decimal]$row[$costIndex]
                
                if($DaysBack -gt 1) {
                    $originalCost = $originalCost / $DaysBack
                }    

                # Pick rate map; default to 1:1 if currency not in the table
                $rateMap = $exchangeRates[$billingCurrency]
                if (-not $rateMap) { $rateMap = @{ 'USD' = 1.0; 'EUR' = 1.0 } }

                # Convert to USD and EUR based on billing currency
                $costUSD = if ($billingCurrency -eq 'USD') { 
                    $originalCost 
                } else { 
                    $originalCost * [decimal]$rateMap['USD']
                }
                
                $costEUR = if ($billingCurrency -eq 'EUR') { 
                    $originalCost 
                } else { 
                    $originalCost * [decimal]$rateMap['EUR']
                }

                $costData += [PSCustomObject]@{
                    CostDate            = $row[$dateIndex]
                    BillingCurrency     = $billingCurrency
                    PreTaxCostOriginal  = [decimal]::Round($originalCost, 4)
                    PreTaxCostUSD       = [decimal]::Round($costUSD, 4)
                    PreTaxCostEUR       = [decimal]::Round($costEUR, 4)
                }
            }

            return $costData

        } else {
            Write-Verbose "No cost data returned for storage account: $($StorageAccountResourceId)"
            return $null
        }
    } catch {
        Write-Error "Error retrieving daily costs for storage account $($StorageAccountResourceId): $_"
        return $null
    }
} #end function Get-StorageAccountDailyCost


function Get-StorageAccountsInBackupConfig {
    <#
    .SYNOPSIS
    Returns storage accounts configured for Azure Backup.

    .DESCRIPTION
    Authenticates to Log Analytics using the provided service principal, runs a Kusto
    query against the specified workspace to find backup-protected Azure Storage
    accounts, and returns their names.

    .PARAMETER clientId
    Service principal application ID used to get a Log Analytics access token.

    .PARAMETER clientSecret
    Secret for the service principal used to get a Log Analytics access token.

    .PARAMETER tenantId
    Azure AD tenant containing the service principal and Log Analytics workspace.

    .OUTPUTS
    System.String[] of storage account names configured for Azure Backup.
    #>
    param(
        [Parameter(Mandatory = $true)]
        [string]$clientId,
        [Parameter(Mandatory = $true)]
        [string]$clientSecret,
        [Parameter(Mandatory = $true)]
        [string]$tenantId
    )

    $workspaceId  = "2eccf0ae-b44c-42b0-a706-8f548f1b0d9d" 

    $bodyT = @{
        grant_type    = "client_credentials"
        client_id     = $clientId
        client_secret = $clientSecret
        resource      = "https://api.loganalytics.io"
    }
    
    try {
        $tokenResponse = Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$tenantId/oauth2/token" -Body $bodyT
        $tokenT = $tokenResponse.access_token
    } catch {
        Write-Error "Failed to obtain access token: $_ ; terminating script"
        exit 1
    }

$query = @"
    AddonAzureBackupProtectedInstance 
    | where BackupManagementType == 'AzureStorage'
    | distinct ProtectedContainerUniqueId
    | project ProtectedStorageAccount = split(ProtectedContainerUniqueId,';')[-1]
"@

    $uri = "https://api.loganalytics.io/v1/workspaces/$workspaceId/query"
    $headers = @{
        Authorization = "Bearer $tokenT"
        "Content-Type" = "application/json"
    }

    $bodyQ = @{
        query = $query
    } | ConvertTo-Json -Depth 3

    $protectedStorageAccounts = @()
    try {
        $response = Invoke-RestMethod -Method Post -Uri $uri -Headers $headers -Body $bodyQ
        if ($response.tables[0].rows) {
            foreach ($row in $response.tables[0].rows) {                
                if ($row[0]) {
                    $protectedStorageAccounts += $row[0]
                }
            }
        }
    } catch {        
        Write-Error "Error querying Log Analytics workspace: $_"
        $protectedStorageAccounts = @()
    }
    
    return $protectedStorageAccounts

} # end function Get-StorageAccountsInBackupConfig


$theAccountList = New-Object -TypeName System.Collections.Arraylist

function Get-AzStorageAccountList {
    <#
    .SYNOPSIS
    Retrieves all Azure Storage accounts across subscriptions using Azure Resource Graph.
    
    .DESCRIPTION
    Queries Azure Resource Graph to get a list of all storage accounts with their
    resource group and subscription information. Handles pagination automatically.
    
    .PARAMETER PageSize
    Number of results to retrieve per page. Default is 100.
    
    .OUTPUTS
    Array of storage account objects with name, resourceGroup, and subscriptionId properties.
    
    .EXAMPLE
    $accounts = Get-AzStorageAccountList
    $accounts = Get-AzStorageAccountList -PageSize 50
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [int]$PageSize = 100
    )
    
    $accountList = @()
    $skipToken = $null

    do {
        $params = @{
            Query = "resources | where type =~ 'microsoft.storage/storageaccounts' | join kind=inner ( resourcecontainers | where type =~ 'microsoft.resources/subscriptions/resourcegroups' | project subscriptionId, resourceGroup) on subscriptionId, resourceGroup  | project name, resourceGroup, subscriptionId"
            First = $PageSize
        }
        if ($skipToken) {
            $params['SkipToken'] = $skipToken
        }

        $result = Search-AzGraph @params
        if ($null -ne $result -and $result.Count -gt 0) {
            $accountList += $result
            $skipToken = $result.SkipToken
        } else {
            break
        }
    } while ($skipToken)
    
    return $accountList
} #end function Get-AzStorageAccountList

$accountList = Get-AzStorageAccountList


function Get-AzStorageAccountExtendedProperties {
    <#
    .SYNOPSIS
    Retrieves extended properties for Azure Storage accounts using Azure Resource Graph.
    
    .DESCRIPTION
    Queries Azure Resource Graph to get extended properties for storage accounts including
    firewall settings, minimum TLS version, and hierarchical namespace enablement status.
    Handles pagination automatically.
    
    .PARAMETER PageSize
    Number of results to retrieve per page. Default is 100.
    
    .OUTPUTS
    Array of storage account objects with extended properties including name, location,
    firewallSettings, minimumTlsVersion, and isHnsEnabled.
    
    .EXAMPLE
    $extendedProps = Get-AzStorageAccountExtendedProperties
    $extendedProps = Get-AzStorageAccountExtendedProperties -PageSize 50
    #>
    
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $false)]
        [int]$PageSize = 100
    )
    
    $accountAzGraphTbl = @()
    $skipTokenAzGraph = $null

    do {
        $params = @{
            Query = "resources | where type == 'microsoft.storage/storageaccounts' | extend firewallSettings = case( properties.publicNetworkAccess == 'Disabled', 'Disabled', isempty(properties.publicNetworkAccess) or (properties.publicNetworkAccess == 'Enabled' and properties.networkAcls.defaultAction == 'Allow'),  'all networks allowed',  'selected virtual networks and IP addresses') | project name, resourceGroup, subscriptionId, location, firewallSettings, minimumTlsVersion = properties.minimumTlsVersion, isHnsEnabled = properties.isHnsEnabled"
            First = $PageSize
        }
        if ($skipTokenAzGraph) {
            $params['SkipToken'] = $skipTokenAzGraph
        }

        $resultAzGraph = Search-AzGraph @params
        if ($null -ne $resultAzGraph -and $resultAzGraph.Count -gt 0) {
            $accountAzGraphTbl += $resultAzGraph
            $skipTokenAzGraph = $resultAzGraph.SkipToken
        } else {
            break
        }
    } while ($skipTokenAzGraph)
    
    return $accountAzGraphTbl
} #end function Get-AzStorageAccountExtendedProperties


function Get-AzStorageAccountNetworkSecurityPerimeterInfo {
    <#
    .SYNOPSIS
    Retrieves Network Security Perimeter configurations for a specific storage account.

    .DESCRIPTION
    Queries the Azure Resource Manager API to retrieve Network Security Perimeter
    configurations associated with a given storage account using managed identity
    authentication and ARM headers. Also retrieves the friendly name of the NSP resource
    by querying the NSP resource directly.

    .PARAMETER StorageAccountResourceId
    The full Azure resource ID of the storage account.

    .PARAMETER Headers
    The authorization headers (hashtable) obtained from Get-ManagedIdentityArmHeaders.

    .OUTPUTS
    Array of PSCustomObject with properties: Name (friendly NSP name), PerimeterGuid, ProvisioningState.
    Returns empty array if no perimeters are found or on error.

    .EXAMPLE
    $perimeters = Get-AzStorageAccountNetworkSecurityPerimeterInfo -StorageAccountResourceId $accountId -Headers $headers
    #>
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [string]$StorageAccountResourceId,
        [Parameter(Mandatory = $true)]
        [hashtable]$Headers
    )

    try {
        $accountIdPath = if ($StorageAccountResourceId.StartsWith('/')) { $StorageAccountResourceId } else { "/$StorageAccountResourceId" }
        $uri = "https://management.azure.com$accountIdPath/networkSecurityPerimeterConfigurations?api-version=2023-05-01"

        $response = Invoke-RestMethod -Uri $uri -Headers $Headers -Method Get -ErrorAction Stop

        if ($response.value -and $response.value.Count -gt 0) {
            $perimeters = @()
            foreach ($perimeter in $response.value) {
                $nspResourceId = $perimeter.properties.networkSecurityPerimeter.id
                $nspFriendlyName = $null
                
                # Retrieve the NSP resource details to get the friendly name
                if ($nspResourceId) {
                    try {
                        $nspUri = "https://management.azure.com$nspResourceId`?api-version=2023-08-01"
                        $nspResource = Invoke-RestMethod -Uri $nspUri -Headers $Headers -Method Get -ErrorAction Stop
                        $nspFriendlyName = $nspResource.name
                        Write-Verbose "Retrieved NSP friendly name: $nspFriendlyName for resource ID: $nspResourceId"
                    } catch {
                        Write-Verbose "Could not retrieve NSP resource details for $nspResourceId : $_"
                        # Fallback: extract name from resource ID if API call fails
                        $nspFriendlyName = $nspResourceId.Split('/')[-1]
                    }
                }
                
                $perimeters += [PSCustomObject]@{
                    Name              = $nspFriendlyName                    
                    ProvisioningState = $perimeter.properties.provisioningState
                }
            }
            return $perimeters
        } else {
            Write-Verbose "No Network Security Perimeters linked to storage account: $StorageAccountResourceId"
            return @()
        }
    } catch {
        Write-Error "Error retrieving Network Security Perimeter info for $($StorageAccountResourceId): $_"
        return @()
    }
} #end function Get-AzStorageAccountNetworkSecurityPerimeterInfo


$accountAzGraphTbl = Get-AzStorageAccountExtendedProperties

$subScriptions = $accountList | Group-Object -Property subscriptionId | Select-Object -ExpandProperty Name

write-output "We have $($subScriptions.count) subscriptions with storage accounts"

foreach ($sub in $subScriptions) {


    $subAccounts = $accountList | Where-Object {$_.subscriptionId -eq $sub}
    write-output "Sub $sub has accounts no: $($subAccounts.count)"

    Set-AzContext -Subscription $sub

    foreach ($sta in $subAccounts) {            
        
        try {
            
            $account   = Get-AzStorageAccount  -ResourceGroupName $sta.resourceGroup -Name $sta.name
            $accountId = $account | select-object -expandproperty id            
            $StartTime = (Get-Date).AddHours(-2).ToString("HH:mm:ss")
            $EndTime   = (Get-Date).AddHours(-1).ToString("HH:mm:ss")
            $sizeSta   = $null
                       
            try {
                 $sizeSta = (Get-AzMetric -ResourceId $accountId -MetricName "UsedCapacity" -AggregationType Maximum -StartTime $StartTime -EndTime $EndTime -TimeGrain 01:00:00 -WarningAction SilentlyContinue).Data | Select-Object -Expandproperty Maximum
                if ($null -eq $sizeSta -or $sizeSta -eq 0 -or  $sizeSta -eq "" -or $sizeSta -notmatch "\d") {   
                    Start-Sleep -Milliseconds 500              
                    $StartTime = (Get-Date).AddHours(-30).ToString("HH:mm:ss")
                    $EndTime   = (Get-Date).AddHours(-1).ToString("HH:mm:ss")
                    $sizeSta   = (Get-AzMetric -ResourceId $accountId -MetricName "UsedCapacity" -StartTime $StartTime -EndTime $EndTime -TimeGrain 01:00:00 -AggregationType Total -WarningAction SilentlyContinue).Data | Select-Object -Expandproperty Total
                }    
            } catch {
                $sizeSta = 1                
            }            
           
            if ($null -eq $sizeSta) {
                $sizeSta = 1
            }

            try {
                $transactionsSta = (Get-AzMetric -ResourceId $accountId -MetricName "Transactions" -AggregationType Total -StartTime $StartTime -EndTime $EndTime -TimeGrain 01:00:00 -WarningAction SilentlyContinue).Data | Select-Object -Expandproperty Total
                if ($null -eq $transactionsSta -or $transactionsSta -eq 0 -or $transactionsSta -eq "" -or $transactionsSta -notmatch "\d") {
                    Start-Sleep -Milliseconds 500                    
                    $StartTime       = (Get-Date).AddHours(-30).ToString("HH:mm:ss")
                    $EndTime         = (Get-Date).AddHours(-1).ToString("HH:mm:ss")
                    $transactionsSta = (Get-AzMetric -ResourceId $accountId -MetricName "Transactions" -StartTime $StartTime -EndTime $EndTime -TimeGrain 01:00:00 -AggregationType Total -WarningAction SilentlyContinue).Data | Select-Object -Expandproperty Total
                }
            } catch {
                $transactionsSta = 0
            }

            if ($null -eq $transactionsSta) {
                $transactionsSta = 0
            }

            if ($transactionsSta.count -gt 1) {
                $transactionsSta = $transactionsSta[0]
            }

            
            try {                
                $storageAccount        = Get-AzStorageAccount -ResourceGroupName $sta.resourceGroup -Name $sta.name
                $blobServiceProperties = Get-AzStorageBlobServiceProperty -StorageAccount $storageAccount               
            
                try {                    
                    $anonymousAccessStatus = if ($storageAccount.AllowBlobPublicAccess) { "True" } else { "False" }
                } catch {
                    Write-Error -Message "Error checking anonymous access for $($sta.name): $_"
                    $anonymousAccessStatus = "False"
                }
                
                $retentionPolicy = $blobServiceProperties.DeleteRetentionPolicy
                $isRetentionEnabled = if ($retentionPolicy.Enabled) { "True" } else { "False" }
                                
                $retentionDays = if ($isRetentionEnabled -eq "True") { $retentionPolicy.Days } else { 0 }
                                
                $versioning = $blobServiceProperties.IsVersioningEnabled
                $isVersioningEnabled = if ($versioning) { "True" } else { "False" }
                                
                $versioningDays = 0
                if ($isVersioningEnabled -eq "True") {                    
                    if ($null -ne $blobServiceProperties.RestorePolicy -and 
                        $blobServiceProperties.RestorePolicy.Enabled) {
                        $versioningDays = $blobServiceProperties.RestorePolicy.Days
                    } else {                        
                        $versioningDays = 0
                    }
                }
            }
            catch {
                Write-Error -Message "Error retrieving blob properties for $($sta.name): $_"
                $anonymousAccessStatus = "False"
                $isRetentionEnabled = "False"
                $retentionDays = 0
                $isVersioningEnabled = "False"
                $versioningDays = 0
            }            

            try {
                # Get static website status directly from blob service properties
                $staticWebsiteStatus = Get-AzStorageServiceProperty -ServiceType Blob -Context $account.Context
                $hasWebEndPoint = if ($null -ne $staticWebsiteStatus -and 
                                     $null -ne $staticWebsiteStatus.StaticWebsite -and 
                                     $staticWebsiteStatus.StaticWebsite.Enabled -eq $true) {
                    "True"
                } else {
                    "False"
                }

                if ($hasWebEndPoint -eq "True") {
                    Write-Output "Static website enabled for $($sta.name)"
                }
            } catch {
                Write-Error -Message "Error checking static website for $($sta.name): $_"
                $hasWebEndPoint = "False"
            }

            if ($sizeSta.count -gt 1) {
                $sizeSta = $sizeSta[0]
            }

            try {                
                $azureFileShares = Get-AzStorageShare -Context $account.Context | Where-Object { -not $_.SnapshotTime } -ErrorAction Stop                
                if ($azureFileShares -and $azureFileShares.Count -gt 0) {
                    $isAzureFilesUsed = "True"
                    $AzureFileshareCount = $azureFileShares.Count
                } else {
                    $isAzureFilesUsed = "False"
                    $AzureFileshareCount = 0
                }
            } catch {
                Write-Error -Message "Error checking Azure Files for $($sta.name): $_"
                $isAzureFilesUsed = "False"
                $AzureFileshareCount = 0
            }
            
            $redundancyType = $storageAccount.Sku.Name          
            $accessTier     = $storageAccount.AccessTier
                        
            try {
                
                $accountIdPath = if ($accountId.StartsWith('/')) { $accountId } else { "/$accountId" }                
                
                $defenderApiUrl   = "https://management.azure.com$accountIdPath/providers/Microsoft.Security/advancedThreatProtectionSettings/current?api-version=2019-01-01"                                                      
                $defenderResponse = Invoke-RestMethod -Uri $defenderApiUrl -Headers $headers -Method Get -ErrorAction Stop               
            
                if ($null -ne $defenderResponse.properties) {
                    $defenderEnabled = if ($defenderResponse.properties.isEnabled -eq $true) { "True" } else { "False" }                    
                } else {
                    $defenderEnabled = "False"
                    Write-Output "Defender response received but no properties found."
                }
            } catch {
                $statusCode = $_.Exception.Response.StatusCode.value__
                Write-Error "Error retrieving Defender status for $($sta.name): StatusCode=$statusCode, Message=$($_.Exception.Message)"
                $defenderEnabled = "Unknown"
            }            
            
            $sftpEnabled = if ($storageAccount.EnableSftp) { "True" } else { "False" }
                        
            try {
                $azGraphProps = $accountAzGraphTbl | Where-Object { $_.name -eq $sta.name -and $_.resourceGroup -eq $sta.resourceGroup -and $_.subscriptionId -eq $sub }
                if ($azGraphProps) {
                    $firewallSettings = $azGraphProps.firewallSettings
                    $minimumTlsVersion = $azGraphProps.minimumTlsVersion
                    $location = $azGraphProps.location
                    $hierarchicalNamespaceEnabled = if ($azGraphProps.isHnsEnabled) { "True" } else { "False" }
                } else {
                    $firewallSettings = "Unknown"
                    $minimumTlsVersion = "Unknown"
                    $location = "Unknown"
                    $hierarchicalNamespaceEnabled = "Unknown"
                }

                if (-not $minimumTlsVersion -or $minimumTlsVersion -eq "Unknown") {
                    $minimumTlsVersion = $storageAccount.MinimumTlsVersion
                }

                if (-not $location -or $location -eq "Unknown") {
                    $location = $storageAccount.Location
                }
            } catch {
                $firewallSettings = "Unknown"
                $minimumTlsVersion = "Unknown"
                $location = "Unknown"
                $hierarchicalNamespaceEnabled = "Unknown"
            }            
            
            $protectedAccounts = Get-StorageAccountsInBackupConfig -clientId $PowershellReadAccessClientIDVal -clientSecret $PowershellReadAccessClientSecretVal -tenantId $TenantId

            if ($protectedAccounts -ne "empty" -and $null -ne $protectedAccounts) {                
                $protectedAccountsList = @()
                if ($protectedAccounts -is [System.Collections.IEnumerable] -and -not ($protectedAccounts -is [string])) {
                    $protectedAccountsList = $protectedAccounts | ForEach-Object { $_.ToString() }
                } elseif ($protectedAccounts -is [string]) {
                    $protectedAccountsList = @($protectedAccounts)
                }
                if ($protectedAccountsList -contains $sta.name) {
                    $IsConfiguredForBackup = "True"
                } else {
                    $IsConfiguredForBackup = "False"
                }
            } else {
                $IsConfiguredForBackup = "Na"
            }                      
            
            # Retrieve daily costs for the storage account (look back 7 days; Cost Mgmt data can lag 24-48h)
            $dailyCostSumUSD = 0.0001
            $dailyCostSumEUR = 0.0001
            $dailyCostSumOriginal = 0.0001
            $billingCurrency = "Unknown"
            try {
                if ($null -ne $headers) {
                    $dailyCosts = Get-StorageAccountDailyCost -StorageAccountResourceId $accountId -SubscriptionId $sub -Headers $headers -DaysBack 7
                    if ($null -ne $dailyCosts) {
                        $dailyCostSumUSD = ($dailyCosts | Measure-Object -Property PreTaxCostUSD -Sum).Sum
                        $dailyCostSumEUR = ($dailyCosts | Measure-Object -Property PreTaxCostEUR -Sum).Sum
                        $dailyCostSumOriginal = ($dailyCosts | Measure-Object -Property PreTaxCostOriginal -Sum).Sum
                        $billingCurrency = $dailyCosts[0].BillingCurrency
                        if ($null -eq $dailyCostSumUSD -or $dailyCostSumUSD -eq 0) {
                            $dailyCostSumUSD = 0.0001
                        }
                        if ($null -eq $dailyCostSumEUR -or $dailyCostSumEUR -eq 0) {
                            $dailyCostSumEUR = 0.0001
                        }
                        if ($null -eq $dailyCostSumOriginal -or $dailyCostSumOriginal -eq 0) {
                            $dailyCostSumOriginal = 0.0001
                        }                         
                    } else {
                        Write-Verbose "No cost data returned for $($sta.name)"
                    }
                } else {
                    Write-Error "Skipping cost retrieval for $($sta.name) because ARM headers are null"
                }
            }
            catch {
                Write-Error "Error retrieving daily costs for $($sta.name): $_"
                $dailyCostSumUSD = 0.0001
                $dailyCostSumEUR = 0.0001
                $dailyCostSumOriginal = 0.0001
            } finally {
                if ($null -eq $dailyCostSumUSD) {
                    $dailyCostSumUSD = 0.0001
                }
                if ($null -eq $dailyCostSumEUR) {
                    $dailyCostSumEUR = 0.0001
                }
                if ($null -eq $dailyCostSumOriginal) {
                    $dailyCostSumOriginal = 0.0001
                }
                
                $dailyCostSumUSD      = $dailyCostSumUSD.ToString()
                $dailyCostSumEUR      = $dailyCostSumEUR.ToString()
                $dailyCostSumOriginal = $dailyCostSumOriginal.ToString()
                
                if ($dailyCostSumUSD -match '[\.,](\d{5,})') {
                    $dailyCostSumUSD = $dailyCostSumUSD -replace '[\.,](\d{4})\d+', '.$1'
                }
                if ($dailyCostSumEUR -match '[\.,](\d{5,})') {
                    $dailyCostSumEUR = $dailyCostSumEUR -replace '[\.,](\d{4})\d+', '.$1'
                }
                if ($dailyCostSumOriginal -match '[\.,](\d{5,})') {
                    $dailyCostSumOriginal = $dailyCostSumOriginal -replace '[\.,](\d{4})\d+', '.$1'
                }
                #Write-Output "Name: $($sta.Name) => Daily cost dailyCostSumOriginal = $dailyCostSumOriginal, type = $($dailyCostSumOriginal.GetType().FullName) dailyCostSumUSD = $dailyCostSumUSD, type = $($dailyCostSumUSD.GetType().FullName) dailyCostSumEUR = $dailyCostSumEUR, type = $($dailyCostSumEUR.GetType().FullName)"
            }
            
            try {
                $nspInfo = Get-AzStorageAccountNetworkSecurityPerimeterInfo -StorageAccountResourceId $accountId -Headers $headers
                $nspName = ""
                $nspProvisioningState = ""
                if ($nspInfo -and $nspInfo.Count -gt 0) {                    
                    foreach ($nsp in $nspInfo) {                    
                        $nspName = if ($nsp.Name) { $nsp.Name } else { "Not linked." }
                        $nspProvisioningState = if ($nsp.ProvisioningState) { $nsp.ProvisioningState } else { "NA" }
                    }                    
                } else {
                    $nspName = "Not linked."
                    $nspProvisioningState = "NA"                    
                }                
            } catch {
                write-Error "Error retrieving Network Security Perimeter info for $($sta.name): $_"
            }

            $dataHsh = @{                
                'StorageAccount'                            = $($sta.Name)                                   
                'SizeInBytes'                               = $sizeSta
                'Transactions'                              = $transactionsSta    
                'IsAnonymousAccessEnabled'                  = $anonymousAccessStatus
                'IsRetentionEnabled'                        = $isRetentionEnabled
                'RetentionDays'                             = $retentionDays
                'IsVersioningEnabled'                       = $isVersioningEnabled
                'VersioningDays'                            = $versioningDays
                'IsAzureFilesUsed'                          = $isAzureFilesUsed
                'AzureFileshareCount'                       = $AzureFileshareCount
                'RedundancyType'                            = $redundancyType
                'AccessTier'                                = $accessTier 
                'IsDefenderEnabled'                         = $defenderEnabled 
                'IsSFTPEnabled'                             = $sftpEnabled
                'Location'                                  = $location
                'FirewallSettings'                          = $firewallSettings
                'HierarchicalNamespaceEnabled'              = $hierarchicalNamespaceEnabled
                'MinimumTlsVersion'                         = $minimumTlsVersion
                'IsConfiguredForBackup'                     = $IsConfiguredForBackup
                'BillingCurrency'                           = $billingCurrency
                'DailyCostSumOriginal'                      = $dailyCostSumOriginal -as [string]
                'DailyCostSumUSD'                           = $dailyCostSumUSD -as [string]
                'DailyCostSumEUR'                           = $dailyCostSumEUR -as [string]
                'HasStaticWebsiteEndPoint'                  = $hasWebEndPoint -as [string]
                'NetworkSecurityPerimeterName'              = $nspName -as [string]
                'NetworkSecurityPerimeterProvisioningState' = $nspProvisioningState -as [string]
            }

            $obj = New-Object -Property $dataHsh -TypeName PSCustomObject

            $null = $theAccountList.add($obj)          
 
        } catch {
            
            Write-Output $_.Exception.Message
            "An error occured for Account $($sta.name)"

        }      
        

    }    

}


$global:AzDceDetails = Get-AzDceListAll -AzAppId $LogIngestAppId `
                                        -AzAppSecret $LogIngestAppSecret `
                                        -TenantId $TenantId `
                                        -Verbose:$Verbose   

$global:AzDcrDetails = Get-AzDcrListAll -AzAppId $LogIngestAppId `
                                        -AzAppSecret $LogIngestAppSecret `
                                        -TenantId $TenantId `
                                        -Verbose:$Verbose

$tmpArray = @()

foreach ($item in $theAccountList) {
    $itmObj = New-Object psobject -Property @{
        SizeInBytes                               = [long]$item.SizeInBytes        
        StorageAccount                            = [string]$item.StorageAccount        
        Transactions                              = [long]$item.Transactions
        IsAnonymousAccessEnabled                  = [string]$item.IsAnonymousAccessEnabled
        IsRetentionEnabled                        = [string]$item.IsRetentionEnabled
        RetentionDays                             = [long]$item.RetentionDays
        IsVersioningEnabled                       = [string]$item.IsVersioningEnabled
        VersioningDays                            = [long]$item.VersioningDays
        IsAzureFilesUsed                          = [string]$item.IsAzureFilesUsed
        AzureFileshareCount                       = [long]$item.AzureFileshareCount
        RedundancyType                            = [string]$item.RedundancyType
        AccessTier                                = [string]$item.AccessTier
        IsDefenderEnabled                         = [string]$item.IsDefenderEnabled
        IsSFTPEnabled                             = [string]$item.IsSFTPEnabled
        Location                                  = [string]$item.Location
        FirewallSettings                          = [string]$item.FirewallSettings
        MinimumTlsVersion                         = [string]$item.MinimumTlsVersion
        HierarchicalNamespaceEnabled              = [string]$item.HierarchicalNamespaceEnabled
        IsConfiguredForBackup                     = [string]$item.IsConfiguredForBackup
        BillingCurrency                           = [string]$item.BillingCurrency
        DailyCostAvgOriginal                      = [string]$item.DailyCostSumOriginal
        DailyCostAvgUSD                           = [string]$item.DailyCostSumUSD
        DailyCostAvgEUR                           = [string]$item.DailyCostSumEUR
        HasStaticWebsiteEndPoint                  = [string]$item.HasStaticWebsiteEndPoint
        NetworkSecurityPerimeterName              = [string]$item.NetworkSecurityPerimeterName
        NetworkSecurityPerimeterProvisioningState = [string]$item.NetworkSecurityPerimeterProvisioningState
    }
    $tmpArray += $itmObj
}

$Datavariable = $tmpArray 
#$Datavariable = $tmpArray | Select-Object -First 5

$AzLogDcrTableCreateFromReferenceMachine      = @()

$DataVariable = ValidateFix-AzLogAnalyticsTableSchemaColumnNames -Data $DataVariable -Verbose:$Verbose

$DataVariable = Build-DataArrayToAlignWithSchema -Data $DataVariable -Verbose:$Verbose

<#
# Creating or updating DCR structure if properties are added or removed
If ($DataVariable)
{
        $ResultMgmt = CheckCreateUpdate-TableDcr-Structure -AzLogWorkspaceResourceId $LogAnalyticsWorkspaceResourceId `
                                                        -AzAppId $LogIngestAppId `
                                                        -AzAppSecret $LogIngestAppSecret `
                                                        -TenantId $TenantId `
                                                        -DceName $AzDceName `
                                                        -DcrName $AzDcrName `
                                                        -DcrResourceGroup $AzDcrResourceGroup `
                                                        -TableName $TableName `
                                                        -Data $DataVariable `
                                                        -LogIngestServicePricipleObjectId $AzDcrLogIngestServicePrincipalObjectId `
                                                        -AzDcrSetLogIngestApiAppPermissionsDcrLevel $AzDcrSetLogIngestApiAppPermissionsDcrLevel `
                                                        -AzLogDcrTableCreateFromAnyMachine $AzLogDcrTableCreateFromAnyMachine `
                                                        -AzLogDcrTableCreateFromReferenceMachine $AzLogDcrTableCreateFromReferenceMachine `
                                                        -Verbose:$Verbose
    
} # If $DataVariable
#>


# Posting data to Log Analytics via DCR/DCE if data is available
if ($DataVariable) {
    $null = Post-AzLogAnalyticsLogIngestCustomLogDcrDce-Output -DceName $AzDceName `
                                                                        -DcrName $AzDcrName `
                                                                        -Data $DataVariable `
                                                                        -TableName $TableName `
                                                                        -AzAppId $LogIngestAppId `
                                                                        -AzAppSecret $LogIngestAppSecret `
                                                                        -TenantId $TenantId `
                                                                        -BatchAmount 500 `
                                                                        -Verbose:$Verbose

    Write-Output "Data posted to Log Analytics successfully"
}

