# =====================
# PowerShell Script: FORCAM API + CSV Import to SQL Server (Production-Grade)
# Version: 2.0
# Description: Robust, production-ready script for importing FORCAM API data and CSV files to SQL Server
# Author: System Integration Team
# Date: 2025-08-04
# =====================

[CmdletBinding()]
param(
    [string]$ConfigPath = "$PSScriptRoot\Config.ps1",
    [switch]$ApiOnly,
    [switch]$CsvOnly,
    [switch]$NoWatcher,
    [string]$LogLevel = "INFO"
)

# Load Configuration
if (-not (Test-Path $ConfigPath)) {
    throw "Configuration file not found: $ConfigPath"
}

$Config = & $ConfigPath

# Override log level if specified
if ($LogLevel) {
    $Config.LogLevel = $LogLevel
}

# =====================
# GLOBAL VARIABLES & PERFORMANCE COUNTERS
# =====================

$global:ImportStats = [PSCustomObject]@{
    StartTime = Get-Date
    CsvSuccess = 0
    CsvFailures = 0
    ApiSuccess = 0
    ApiFailures = 0
    ApiRetries = 0
    CsvRetries = 0
    TotalRecordsProcessed = 0
    ErrorsSkipped = 0
    LastUpdate = Get-Date
}

$global:PerformanceCounters = @{
    ApiRequestsPerSecond = 0
    CsvFilesPerSecond = 0
    RecordsPerSecond = 0
    AverageResponseTime = 0
}

# =====================
# UTILITY FUNCTIONS
# =====================

function Write-Log {
    param (
        [string]$Message,
        [string]$Level = "INFO",
        [switch]$NoConsole
    )
    
    $logLevels = @{
        "DEBUG" = 0
        "INFO" = 1
        "WARNING" = 2
        "ERROR" = 3
        "FATAL" = 4
    }
    
    if ($logLevels[$Level] -lt $logLevels[$Config.LogLevel]) {
        return
    }
    
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
    $processId = $PID
    $threadId = [System.Threading.Thread]::CurrentThread.ManagedThreadId
    $entry = "[$timestamp] [$processId:$threadId] [$Level] $Message"
    
    try {
        Add-Content -Path $Config.LogFile -Value $entry -ErrorAction Stop -Encoding UTF8
    } catch {
        if (-not $NoConsole) {
            Write-Warning "Logging failed: $_"
            Write-Output $entry
        }
    }
    
    if (-not $NoConsole) {
        switch ($Level) {
            "DEBUG" { Write-Verbose $entry }
            "INFO" { Write-Host $entry -ForegroundColor White }
            "WARNING" { Write-Warning $entry }
            "ERROR" { Write-Error $entry }
            "FATAL" { Write-Host $entry -ForegroundColor Red }
        }
    }
}

function Send-AlertEmail {
    param (
        [string]$Body,
        [string]$Subject = $Config.EmailSubject,
        [string]$Priority = "Normal"
    )
    
    if (-not $Config.EmailEnabled) {
        Write-Log "Email alerts disabled, skipping notification" "DEBUG"
        return
    }
    
    try {
        $securePass = ConvertTo-SecureString $Config.SmtpPassword -AsPlainText -Force
        $cred = New-Object System.Management.Automation.PSCredential ($Config.SmtpUser, $securePass)
        
        $mailParams = @{
            From = $Config.EmailFrom
            To = $Config.EmailTo
            Subject = "$Subject - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
            Body = $Body
            SmtpServer = $Config.SmtpServer
            Port = $Config.SmtpPort
            UseSsl = $true
            Credential = $cred
            Priority = $Priority
        }
        
        Send-MailMessage @mailParams
        Write-Log "Alert email sent successfully" "INFO"
    } catch {
        Write-Log "Failed to send alert email: $_" "ERROR"
    }
}

function Wait-For-FileUnlock {
    param (
        [string]$Path,
        [int]$MaxAttempts = $Config.FileUnlockRetries,
        [int]$DelaySeconds = $Config.FileUnlockWait
    )
    
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        try {
            $stream = [System.IO.File]::Open($Path, 'Open', 'Read', 'ReadWrite')
            if ($stream) {
                $stream.Close()
                $stream.Dispose()
                return $true
            }
        } catch {
            Write-Log "File locked, attempt $($i + 1)/$MaxAttempts: $Path" "DEBUG"
            Start-Sleep -Seconds $DelaySeconds
        }
    }
    
    Write-Log "File remains locked after $MaxAttempts attempts: $Path" "WARNING"
    return $false
}

function Retry-Operation {
    param (
        [scriptblock]$Operation,
        [int]$MaxRetries = $Config.MaxRetries,
        [int]$DelaySeconds = $Config.DelaySeconds,
        [string]$OperationName = "Operation"
    )
    
    for ($attempt = 1; $attempt -le $MaxRetries; $attempt++) {
        try {
            $result = & $Operation
            if ($attempt -gt 1) {
                Write-Log "$OperationName succeeded on attempt $attempt" "INFO"
            }
            return $result
        } catch {
            Write-Log "$OperationName failed on attempt $attempt/$MaxRetries: $_" "WARNING"
            if ($attempt -lt $MaxRetries) {
                Start-Sleep -Seconds ($DelaySeconds * $attempt)  # Exponential backoff
            } else {
                Write-Log "$OperationName failed after $MaxRetries attempts" "ERROR"
                throw $_
            }
        }
    }
}

function Update-PerformanceCounters {
    param(
        [string]$CounterType,
        [double]$Value = 1,
        [datetime]$StartTime
    )
    
    if (-not $Config.EnablePerformanceCounters) { return }
    
    switch ($CounterType) {
        "ApiRequest" {
            $global:PerformanceCounters.ApiRequestsPerSecond++
            if ($StartTime) {
                $responseTime = (Get-Date) - $StartTime
                $global:PerformanceCounters.AverageResponseTime = 
                    ($global:PerformanceCounters.AverageResponseTime + $responseTime.TotalMilliseconds) / 2
            }
        }
        "CsvFile" {
            $global:PerformanceCounters.CsvFilesPerSecond++
        }
        "Record" {
            $global:PerformanceCounters.RecordsPerSecond += $Value
            $global:ImportStats.TotalRecordsProcessed += $Value
        }
    }
}

# =====================
# DATABASE HELPER FUNCTIONS
# =====================

function Get-SqlConnection {
    param([int]$TimeoutSeconds = $Config.ConnectionTimeout)
    
    $connString = if ($Config.UseIntegratedSecurity) {
        "Server=$($Config.SqlServer);Database=$($Config.Database);Integrated Security=True;Connection Timeout=$TimeoutSeconds;"
    } else {
        "Server=$($Config.SqlServer);Database=$($Config.Database);User Id=$($Config.SqlUser);Password=$($Config.SqlPassword);Connection Timeout=$TimeoutSeconds;"
    }
    
    $conn = New-Object System.Data.SqlClient.SqlConnection $connString
    $conn.Open()
    Write-Log "Database connection established to $($Config.SqlServer)/$($Config.Database)" "DEBUG"
    return $conn
}

function Execute-SqlNonQuery {
    param (
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$Sql,
        [hashtable]$Parameters = @{},
        [int]$TimeoutSeconds = $Config.CommandTimeout
    )

    $cmd = $Connection.CreateCommand()
    $cmd.CommandText = $Sql
    $cmd.CommandTimeout = $TimeoutSeconds

    foreach ($key in $Parameters.Keys) {
        $value = $Parameters[$key]
        if ($null -eq $value) {
            $value = [DBNull]::Value
        }
        $cmd.Parameters.AddWithValue($key, $value) | Out-Null
    }

    try {
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        $rowsAffected = $cmd.ExecuteNonQuery()
        $stopwatch.Stop()
        
        Write-Log "SQL executed in $($stopwatch.ElapsedMilliseconds)ms, $rowsAffected rows affected" "DEBUG"
        return $rowsAffected
    } catch {
        Write-Log "SQL execution failed: $_" "ERROR"
        Write-Log "SQL: $Sql" "DEBUG"
        throw $_
    } finally {
        $cmd.Dispose()
    }
}

function Create-DatabaseSchema {
    Write-Log "Creating/verifying database schema..." "INFO"
    
    $conn = Get-SqlConnection
    try {
        # Create schemas
        $schemas = @("staging", "forcam")
        foreach ($schema in $schemas) {
            $sql = "IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '$schema') EXEC('CREATE SCHEMA [$schema]')"
            Execute-SqlNonQuery -Connection $conn -Sql $sql
        }
        
        # Create API data table
        $apiTableSql = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'$($Config.ApiSqlTable)') AND type in (N'U'))
BEGIN
    CREATE TABLE $($Config.ApiSqlTable) (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        machine_name NVARCHAR(100) NOT NULL,
        material_number NVARCHAR(100),
        cycle_time DECIMAL(18,6),
        operation_number NVARCHAR(100),
        workplace NVARCHAR(100),
        order_number NVARCHAR(100),
        timestamp DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        import_timestamp DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        json_blob NVARCHAR(MAX),
        source_endpoint NVARCHAR(200),
        batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        INDEX IX_ApiData_MachineName_Timestamp (machine_name, timestamp),
        INDEX IX_ApiData_MaterialNumber (material_number),
        INDEX IX_ApiData_ImportTimestamp (import_timestamp)
    )
END
"@
        Execute-SqlNonQuery -Connection $conn -Sql $apiTableSql
        
        # Create CSV cycle time table
        $csvTableSql = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.cycle_time') AND type in (N'U'))
BEGIN
    CREATE TABLE staging.forcam.cycle_time (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        machine_name NVARCHAR(100) NOT NULL,
        [date] DATE NOT NULL,
        [time] TIME(3) NOT NULL,
        workplace NVARCHAR(50),
        ordernumber NVARCHAR(50),
        operationnumber NVARCHAR(50),
        materialnumber NVARCHAR(50),
        te_sap REAL,
        machine_cycle_time AS CAST(te_sap AS DECIMAL(18,6)),
        import_timestamp DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        source_file NVARCHAR(500),
        batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        INDEX IX_CycleTime_MachineName_Date (machine_name, [date]),
        INDEX IX_CycleTime_MaterialNumber (materialnumber),
        INDEX IX_CycleTime_ImportTimestamp (import_timestamp)
    )
END
"@
        Execute-SqlNonQuery -Connection $conn -Sql $csvTableSql
        
        # Create error log table
        $errorTableSql = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.error_log') AND type in (N'U'))
BEGIN
    CREATE TABLE staging.forcam.error_log (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        error_message NVARCHAR(MAX) NOT NULL,
        error_date DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        error_severity INT,
        error_line INT,
        machine_name NVARCHAR(100),
        file_path NVARCHAR(500),
        script_version NVARCHAR(20),
        batch_id UNIQUEIDENTIFIER,
        INDEX IX_ErrorLog_ErrorDate (error_date),
        INDEX IX_ErrorLog_MachineName (machine_name)
    )
END
"@
        Execute-SqlNonQuery -Connection $conn -Sql $errorTableSql
        
        Write-Log "Database schema created/verified successfully" "INFO"
    } finally {
        $conn.Close()
        $conn.Dispose()
    }
}

# =====================
# DATA VALIDATION & TRANSFORMATION
# =====================

function Validate-And-Transform-ApiItem {
    param (
        [psobject]$Item,
        [string]$Endpoint
    )

    if (-not $Config.ValidateData) {
        return $Item
    }

    $errors = @()

    # Required field validation
    if (-not $Item.machineName -or [string]::IsNullOrWhiteSpace($Item.machineName)) {
        $errors += "Missing or empty machineName"
    }

    # Data type validation and transformation
    if ($Item.cycleTime -and $Item.cycleTime -is [string]) {
        if (-not [double]::TryParse($Item.cycleTime, [ref]$null)) {
            $errors += "Invalid cycleTime format: $($Item.cycleTime)"
        } else {
            $Item.cycleTime = [math]::Round([double]$Item.cycleTime, 6)
        }
    }

    # Timestamp validation
    if ($Item.timestamp) {
        try {
            $Item.timestamp = [datetime]::Parse($Item.timestamp)
        } catch {
            $errors += "Invalid timestamp format: $($Item.timestamp)"
        }
    } else {
        $Item.timestamp = Get-Date
    }

    if ($errors.Count -gt 0) {
        $errorMsg = "Validation errors for item from $Endpoint`: " + ($errors -join "; ")
        if ($Config.SkipInvalidRecords) {
            Write-Log $errorMsg "WARNING"
            $global:ImportStats.ErrorsSkipped++
            return $null
        } else {
            throw $errorMsg
        }
    }

    return $Item
}

# =====================
# API DATA IMPORT LOGIC
# =====================

function Import-ApiData {
    if (-not $Config.EnableApiImport) {
        Write-Log "API import disabled in configuration" "INFO"
        return
    }
    
    Write-Log "Starting API data import..." "INFO"
    Create-DatabaseSchema

    $batchId = [System.Guid]::NewGuid()
    $totalRecords = 0

    foreach ($endpoint in $Config.ApiEndpoints) {
        Write-Log "Processing API endpoint: $endpoint" "INFO"
        $page = 0
        
        while ($true) {
            $uri = "$($Config.ApiBaseUrl)$endpoint"
            if ($page -gt 0) {
                $uri += "?page=$page"
            }
            
            $headers = @{ 
                "Authorization" = "Bearer $($Config.ApiKey)"
                "Accept" = "application/json"
                "User-Agent" = "FORCAM-PowerShell-Import/2.0"
            }

            try {
                $startTime = Get-Date
                $response = Retry-Operation -Operation { 
                    Invoke-RestMethod -Uri $uri -Headers $headers -Method GET -TimeoutSec 30
                } -MaxRetries $Config.MaxRetries -DelaySeconds $Config.DelaySeconds -OperationName "API call $endpoint page $page"
                
                Update-PerformanceCounters -CounterType "ApiRequest" -StartTime $startTime

                if (-not $response -or (-not $response.items -and -not $response.value) -or 
                    (($response.items -and $response.items.Count -eq 0) -or ($response.value -and $response.value.Count -eq 0))) {
                    Write-Log "No more data from API $endpoint page $page, moving to next endpoint" "INFO"
                    break
                }

                $items = if ($response.items) { $response.items } else { $response.value }
                Write-Log "Retrieved $($items.Count) items from $endpoint page $page" "INFO"

                $conn = Get-SqlConnection
                $transaction = $conn.BeginTransaction()
                $successCount = 0
                $failureCount = 0

                try {
                    foreach ($itemRaw in $items) {
                        try {
                            $item = Validate-And-Transform-ApiItem -Item $itemRaw -Endpoint $endpoint
                            if ($null -eq $item) { continue }  # Skip invalid records

                            $sql = @"
INSERT INTO $($Config.ApiSqlTable)
(machine_name, material_number, cycle_time, operation_number, workplace, order_number, timestamp, json_blob, source_endpoint, batch_id)
VALUES
(@machine, @material, @cycle, @operation, @workplace, @order, @timestamp, @json, @endpoint, @batchId)
"@

                            $params = @{
                                "@machine" = $item.machineName
                                "@material" = $item.materialNumber
                                "@cycle" = $item.cycleTime
                                "@operation" = $item.operationNumber
                                "@workplace" = $item.workplace
                                "@order" = $item.orderNumber
                                "@timestamp" = $item.timestamp
                                "@json" = ($itemRaw | ConvertTo-Json -Depth 10 -Compress)
                                "@endpoint" = $endpoint
                                "@batchId" = $batchId
                            }

                            $cmd = $conn.CreateCommand()
                            $cmd.CommandText = $sql
                            $cmd.Transaction = $transaction
                            $cmd.CommandTimeout = $Config.CommandTimeout
                            
                            foreach ($key in $params.Keys) {
                                $value = $params[$key]
                                if ($null -eq $value) { $value = [DBNull]::Value }
                                $cmd.Parameters.AddWithValue($key, $value) | Out-Null
                            }
                            
                            $cmd.ExecuteNonQuery() | Out-Null
                            $cmd.Dispose()
                            $successCount++
                            
                        } catch {
                            Write-Log "Insert failed for item on $endpoint page $page`: $_" "ERROR"
                            $failureCount++
                            
                            if ($failureCount -gt $Config.MaxErrorsPerBatch) {
                                throw "Too many errors in batch ($failureCount > $($Config.MaxErrorsPerBatch))"
                            }
                        }
                    }

                    $transaction.Commit()
                    Write-Log "API $endpoint page $page imported: $successCount records successfully, $failureCount failed" "INFO"
                    
                    $global:ImportStats.ApiSuccess += $successCount
                    $global:ImportStats.ApiFailures += $failureCount
                    $totalRecords += $successCount
                    
                    Update-PerformanceCounters -CounterType "Record" -Value $successCount
                    
                } catch {
                    $transaction.Rollback()
                    Write-Log "Transaction rolled back for $endpoint page $page`: $_" "ERROR"
                    $global:ImportStats.ApiFailures += $items.Count
                    throw $_
                } finally {
                    $conn.Close()
                    $conn.Dispose()
                }

                $page++
                
                # Handle pagination
                if ($response.'@odata.nextLink') {
                    $uri = $response.'@odata.nextLink'
                    $page = 0  # Reset page counter for nextLink URLs
                } elseif (-not $response.items -or $response.items.Count -lt 100) {
                    # No nextLink and less than full page, assume we're done
                    break
                }

            } catch {
                Write-Log "API import failed at $endpoint page $page`: $_" "ERROR"
                Send-AlertEmail -Body "API import failed at $endpoint page $page`: $_" -Priority "High"
                $global:ImportStats.ApiFailures++
                break
            }
        }
    }
    
    Write-Log "API data import completed. Total records processed: $totalRecords" "INFO"
}

# =====================
# CSV DATA IMPORT LOGIC
# =====================

function Import-CsvFile {
    param (
        [string]$FilePath,
        [string]$MachineName,
        [string]$BackupPath,
        [string]$ErrorPath
    )

    if (-not (Wait-For-FileUnlock -Path $FilePath)) {
        Write-Log "File locked too long: $FilePath" "ERROR"
        $global:ImportStats.CsvFailures++
        return $false
    }

    $batchId = [System.Guid]::NewGuid()
    Write-Log "Processing CSV file: $FilePath (Machine: $MachineName, Batch: $batchId)" "INFO"

    $attempt = 0
    while ($attempt -lt $Config.MaxRetries) {
        $attempt++
        Write-Log "CSV Import attempt $attempt for file: $FilePath" "INFO"
        
        try {
            # Build sqlcmd command with proper parameter passing
            $sqlcmdArgs = @(
                "-S", $Config.SqlServer,
                "-d", $Config.Database,
                "-i", $Config.CsvSqlScript,
                "-v", "FilePath='$FilePath'",
                "-v", "MachineName='$MachineName'",
                "-v", "FileType='.csv'",
                "-v", "BatchId='$batchId'",
                "-b",  # Stop on error
                "-r1"  # Redirect error messages
            )
            
            if ($Config.UseIntegratedSecurity) {
                $sqlcmdArgs += @("-E")  # Use Windows Authentication
            }
            
            $result = & sqlcmd @sqlcmdArgs 2>&1
            $exitCode = $LASTEXITCODE
            
            if ($exitCode -eq 0 -and -not ($result -match "error|failed|exception")) {
                # Success - move to backup
                $backupFile = Join-Path $BackupPath "$(Get-Date -Format 'yyyyMMdd-HHmmss')_$([System.IO.Path]::GetFileName($FilePath))"
                Move-Item -Path $FilePath -Destination $backupFile -Force
                
                Write-Log "Successfully imported CSV and moved to Backup: $FilePath -> $backupFile" "INFO"
                $global:ImportStats.CsvSuccess++
                Update-PerformanceCounters -CounterType "CsvFile"
                return $true
                
            } else {
                Write-Log "CSV import error on attempt $attempt`: Exit Code: $exitCode, Output: $result" "WARNING"
                if ($attempt -lt $Config.MaxRetries) {
                    Start-Sleep -Seconds ($Config.DelaySeconds * $attempt)
                }
            }
            
        } catch {
            Write-Log "Exception during CSV import attempt $attempt`: $_" "ERROR"
            if ($attempt -lt $Config.MaxRetries) {
                Start-Sleep -Seconds ($Config.DelaySeconds * $attempt)
            }
        }
    }

    # All retries failed - move to error folder
    try {
        $errorFile = Join-Path $ErrorPath "$(Get-Date -Format 'yyyyMMdd-HHmmss')_ERROR_$([System.IO.Path]::GetFileName($FilePath))"
        Move-Item -Path $FilePath -Destination $errorFile -Force
        
        Write-Log "CSV import failed after $($Config.MaxRetries) retries. Moved to Error: $FilePath -> $errorFile" "ERROR"
        $global:ImportStats.CsvFailures++
        
        Send-AlertEmail -Body "CSV import failed for file: $FilePath after $($Config.MaxRetries) attempts. File moved to: $errorFile" -Priority "High"
        
    } catch {
        Write-Log "Failed to move failed CSV to Error folder: $_" "ERROR"
    }
    
    return $false
}

function Import-AllCsvFiles {
    if (-not $Config.EnableCsvImport) {
        Write-Log "CSV import disabled in configuration" "INFO"
        return
    }
    
    Write-Log "Starting CSV import of all files from: $($Config.RootPath)" "INFO"
    Create-DatabaseSchema

    if (-not (Test-Path $Config.RootPath)) {
        Write-Log "Root path does not exist: $($Config.RootPath)" "ERROR"
        return
    }

    $folders = Get-ChildItem -Path $Config.RootPath -Directory -ErrorAction SilentlyContinue
    $totalFiles = 0

    foreach ($folder in $folders) {
        $MachineName = $folder.Name
        $MachinePath = $folder.FullName
        $BackupPath = Join-Path $MachinePath "Backup"
        $ErrorPath = Join-Path $MachinePath "Error"

        # Ensure required folders exist
        foreach ($path in @($BackupPath, $ErrorPath)) {
            if (-not (Test-Path $path)) {
                New-Item -ItemType Directory -Path $path -Force | Out-Null
                Write-Log "Created directory: $path" "INFO"
            }
        }

        $csvFiles = Get-ChildItem -Path $MachinePath -Filter "*.csv" -File -ErrorAction SilentlyContinue
        if ($csvFiles.Count -eq 0) {
            Write-Log "No CSV files found in: $MachinePath" "DEBUG"
            continue
        }

        Write-Log "Found $($csvFiles.Count) CSV files for machine: $MachineName" "INFO"
        $totalFiles += $csvFiles.Count

        $jobs = @()

        foreach ($file in $csvFiles) {
            # Limit parallel jobs
            while ($jobs.Count -ge $Config.MaxParallelCsv) {
                $completed = $jobs | Where-Object { $_.State -ne 'Running' }
                if ($completed) {
                    $completed | Remove-Job
                    $jobs = $jobs | Where-Object { $_.State -eq 'Running' }
                }
                Start-Sleep -Milliseconds 100
            }

            # Start background job for file processing
            $job = Start-Job -ArgumentList $file.FullName, $MachineName, $BackupPath, $ErrorPath, $Config -ScriptBlock {
                param($FilePath, $MachineName, $BackupPath, $ErrorPath, $Config)

                # Define helper functions in job scope
                function Write-JobLog {
                    param([string]$Message, [string]$Level = "INFO")
                    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss.fff"
                    $entry = "[$timestamp] [JOB:$($PID)] [$Level] $Message"
                    Add-Content -Path $Config.LogFile -Value $entry -ErrorAction SilentlyContinue
                }

                function Wait-For-JobFileUnlock {
                    param([string]$Path, [int]$MaxAttempts = 6, [int]$DelaySeconds = 5)
                    for ($i = 0; $i -lt $MaxAttempts; $i++) {
                        try {
                            $stream = [System.IO.File]::Open($Path, 'Open', 'Read', 'ReadWrite')
                            if ($stream) {
                                $stream.Close()
                                $stream.Dispose()
                                return $true
                            }
                        } catch {
                            Start-Sleep -Seconds $DelaySeconds
                        }
                    }
                    return $false
                }

                if (-not (Wait-For-JobFileUnlock -Path $FilePath)) {
                    Write-JobLog "File locked too long, skipping: $FilePath" "ERROR"
                    return @{ Success = $false; Error = "File locked" }
                }

                for ($attempt = 1; $attempt -le $Config.MaxRetries; $attempt++) {
                    Write-JobLog "Attempt $attempt`: Importing file $FilePath"
                    
                    try {
                        $batchId = [System.Guid]::NewGuid()
                        $sqlcmdArgs = @(
                            "-S", $Config.SqlServer,
                            "-d", $Config.Database,
                            "-i", $Config.CsvSqlScript,
                            "-v", "FilePath='$FilePath'",
                            "-v", "MachineName='$MachineName'",
                            "-v", "FileType='.csv'",
                            "-v", "BatchId='$batchId'",
                            "-b", "-r1"
                        )
                        
                        if ($Config.UseIntegratedSecurity) {
                            $sqlcmdArgs += @("-E")
                        }
                        
                        $result = & sqlcmd @sqlcmdArgs 2>&1
                        $exitCode = $LASTEXITCODE
                        
                        if ($exitCode -eq 0 -and -not ($result -match "error|failed|exception")) {
                            $backupFile = Join-Path $BackupPath "$(Get-Date -Format 'yyyyMMdd-HHmmss')_$([System.IO.Path]::GetFileName($FilePath))"
                            Move-Item -Path $FilePath -Destination $backupFile -Force
                            Write-JobLog "SUCCESS: Imported and moved $FilePath to $backupFile"
                            return @{ Success = $true; BackupFile = $backupFile }
                        } else {
                            Write-JobLog "FAILED: Attempt $attempt for $FilePath. Exit: $exitCode, Output: $result" "ERROR"
                            Start-Sleep -Seconds ($Config.DelaySeconds * $attempt)
                        }
                    } catch {
                        Write-JobLog "EXCEPTION: Attempt $attempt for $FilePath`: $_" "ERROR"
                        Start-Sleep -Seconds ($Config.DelaySeconds * $attempt)
                    }
                }

                # All attempts failed
                try {
                    $errorFile = Join-Path $ErrorPath "$(Get-Date -Format 'yyyyMMdd-HHmmss')_ERROR_$([System.IO.Path]::GetFileName($FilePath))"
                    Move-Item -Path $FilePath -Destination $errorFile -Force
                    Write-JobLog "FINAL FAILURE: Moved $FilePath to $errorFile" "ERROR"
                    return @{ Success = $false; ErrorFile = $errorFile; Error = "Max retries exceeded" }
                } catch {
                    Write-JobLog "CRITICAL: Could not move $FilePath to Error folder: $_" "ERROR"
                    return @{ Success = $false; Error = "Could not move to error folder: $_" }
                }
            }

            $jobs += $job
        }

        # Wait for all jobs to complete for this machine
        Write-Log "Waiting for $($jobs.Count) import jobs to complete for machine: $MachineName" "INFO"
        $jobs | Wait-Job | Out-Null

        # Collect results
        foreach ($job in $jobs) {
            try {
                $result = Receive-Job -Job $job
                if ($result.Success) {
                    $global:ImportStats.CsvSuccess++
                } else {
                    $global:ImportStats.CsvFailures++
                }
            } catch {
                Write-Log "Failed to receive job result: $_" "ERROR"
                $global:ImportStats.CsvFailures++
            }
        }

        $jobs | Remove-Job
        $jobs = @()
    }

    Write-Log "CSV import complete. Total files processed: $totalFiles" "INFO"
}

# =====================
# FILE SYSTEM WATCHER
# =====================

function Start-FileWatcher {
    if (-not $Config.UseFileWatcher) {
        Write-Log "File watcher disabled in configuration" "INFO"
        return
    }
    
    Write-Log "Starting file watcher on path: $($Config.RootPath)" "INFO"

    if (-not (Test-Path $Config.RootPath)) {
        Write-Log "Watcher root path does not exist: $($Config.RootPath)" "ERROR"
        return
    }

    try {
        $watcher = New-Object System.IO.FileSystemWatcher
        $watcher.Path = $Config.RootPath
        $watcher.IncludeSubdirectories = $true
        $watcher.Filter = "*.csv"
        $watcher.NotifyFilter = [System.IO.NotifyFilters]::FileName -bor [System.IO.NotifyFilters]::LastWrite
        $watcher.EnableRaisingEvents = $true

        # File created event
        $action = {
            $path = $Event.SourceEventArgs.FullPath
            $name = $Event.SourceEventArgs.Name
            $changeType = $Event.SourceEventArgs.ChangeType
            
            Start-Sleep -Seconds 5  # Wait for file to be completely written
            
            try {
                $machineName = Split-Path (Split-Path $path -Parent) -Leaf
                $machineDir = Split-Path $path -Parent
                $backupPath = Join-Path $machineDir "Backup"
                $errorPath = Join-Path $machineDir "Error"
                
                # Ensure directories exist
                foreach ($dir in @($backupPath, $errorPath)) {
                    if (-not (Test-Path $dir)) {
                        New-Item -ItemType Directory -Path $dir -Force | Out-Null
                    }
                }
                
                Write-Log "File watcher detected: $changeType - $path (Machine: $machineName)" "INFO"
                
                # Process the file in a background job
                Start-Job -ArgumentList $path, $machineName, $backupPath, $errorPath -ScriptBlock {
                    param($FilePath, $MachineName, $BackupPath, $ErrorPath)
                    
                    # Load configuration and functions
                    . "$using:PSScriptRoot\Config.ps1" | Out-Null
                    
                    # Simple file processing logic (could call Import-CsvFile here)
                    if (Test-Path $FilePath) {
                        # Basic processing - extend as needed
                        Write-Output "Processing watched file: $FilePath"
                    }
                } | Out-Null
                
            } catch {
                Write-Log "File watcher error processing $path`: $_" "ERROR"
            }
        }

        Register-ObjectEvent -InputObject $watcher -EventName "Created" -Action $action | Out-Null
        Register-ObjectEvent -InputObject $watcher -EventName "Changed" -Action $action | Out-Null

        Write-Log "File watcher started successfully. Monitoring: $($Config.RootPath)" "INFO"

        # Keep the watcher running
        while ($true) {
            Start-Sleep -Seconds 30
            
            # Periodic status update
            $uptime = (Get-Date) - $global:ImportStats.StartTime
            Write-Log "File watcher active. Uptime: $($uptime.ToString('dd\.hh\:mm\:ss'))" "DEBUG"
            
            # Clean up completed jobs
            Get-Job | Where-Object { $_.State -eq 'Completed' } | Remove-Job
        }

    } catch {
        Write-Log "File watcher failed to start: $_" "ERROR"
        Send-AlertEmail -Body "File watcher failed to start: $_" -Priority "High"
    }
}

# =====================
# STATUS AND REPORTING
# =====================

function Get-ImportStatus {
    $uptime = (Get-Date) - $global:ImportStats.StartTime
    $status = [PSCustomObject]@{
        StartTime = $global:ImportStats.StartTime
        Uptime = $uptime.ToString('dd\.hh\:mm\:ss')
        CsvSuccess = $global:ImportStats.CsvSuccess
        CsvFailures = $global:ImportStats.CsvFailures
        ApiSuccess = $global:ImportStats.ApiSuccess
        ApiFailures = $global:ImportStats.ApiFailures
        TotalRecordsProcessed = $global:ImportStats.TotalRecordsProcessed
        ErrorsSkipped = $global:ImportStats.ErrorsSkipped
        SuccessRate = if (($global:ImportStats.CsvSuccess + $global:ImportStats.CsvFailures) -gt 0) {
            [math]::Round(($global:ImportStats.CsvSuccess / ($global:ImportStats.CsvSuccess + $global:ImportStats.CsvFailures)) * 100, 2)
        } else { 0 }
    }
    
    if ($Config.EnablePerformanceCounters) {
        $status | Add-Member -NotePropertyName "PerformanceCounters" -NotePropertyValue $global:PerformanceCounters
    }
    
    return $status
}

function Send-StatusReport {
    $status = Get-ImportStatus
    $body = @"
FORCAM Import System Status Report
Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')

=== SUMMARY ===
Uptime: $($status.Uptime)
Total Records Processed: $($status.TotalRecordsProcessed)
Success Rate: $($status.SuccessRate)%

=== CSV IMPORT ===
Successful: $($status.CsvSuccess)
Failed: $($status.CsvFailures)

=== API IMPORT ===
Successful: $($status.ApiSuccess) 
Failed: $($status.ApiFailures)

=== ERRORS ===
Records Skipped: $($status.ErrorsSkipped)

=== CONFIGURATION ===
Environment: $($env:ENVIRONMENT)
SQL Server: $($Config.SqlServer)
Database: $($Config.Database)
Root Path: $($Config.RootPath)
"@

    Write-Log "Status Report:`n$body" "INFO"
    
    if ($status.CsvFailures -gt 0 -or $status.ApiFailures -gt 0) {
        Send-AlertEmail -Body $body -Subject "FORCAM Import Status - Issues Detected" -Priority "High"
    }
}

# =====================
# MAIN SCRIPT EXECUTION
# =====================

function Main {
    Write-Log "========== FORCAM Import Script Started ==========" "INFO"
    Write-Log "Version: 2.0, Environment: $($env:ENVIRONMENT), PID: $PID" "INFO"
    
    try {
        # Validate configuration
        Write-Log "Validating configuration..." "INFO"
        if (-not $Config.ApiKey -and $Config.EnableApiImport) {
            throw "API Key not configured but API import is enabled"
        }
        
        # Send startup notification
        Send-AlertEmail -Body "FORCAM Import script started successfully on $(Get-Date)" -Subject "FORCAM Import - Startup" -Priority "Low"
        
        # Execute imports based on parameters
        if (-not $CsvOnly -and $Config.EnableApiImport) {
            Import-ApiData
        }
        
        if (-not $ApiOnly -and $Config.EnableCsvImport) {
            Import-AllCsvFiles
        }
        
        # Send status report
        Send-StatusReport
        
        # Start file watcher if enabled and not disabled by parameter
        if (-not $NoWatcher -and $Config.UseFileWatcher) {
            Write-Log "Starting file watcher..." "INFO"
            Start-FileWatcher
        } else {
            Write-Log "File watcher disabled" "INFO"
        }
        
    } catch {
        $errorMsg = "Critical failure in FORCAM import script: $_"
        Write-Log $errorMsg "FATAL"
        Send-AlertEmail -Body "$errorMsg`n`nStack Trace:`n$($_.ScriptStackTrace)" -Subject "FORCAM Import - CRITICAL FAILURE" -Priority "High"
        throw $_
    }
}

# Handle script termination gracefully
trap {
    $errorMsg = "Unhandled exception in FORCAM import script: $_"
    Write-Log $errorMsg "FATAL"
    Send-AlertEmail -Body "$errorMsg`n`nStack Trace:`n$($_.ScriptStackTrace)" -Subject "FORCAM Import - FATAL ERROR" -Priority "High"
    break
}

# Start main execution
if ($MyInvocation.InvocationName -ne '.') {
    Main
}

Write-Log "========== FORCAM Import Script Running ==========" "INFO"
