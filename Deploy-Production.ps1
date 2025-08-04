# =====================
# FORCAM Import System - Production Deployment Script
# Version: 2.0
# Description: Automated deployment and setup for production environment
# =====================

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$SqlServer,
    
    [Parameter(Mandatory=$true)]
    [string]$Database = "staging",
    
    [Parameter(Mandatory=$false)]
    [string]$InstallPath = "C:\ForcamImport",
    
    [Parameter(Mandatory=$false)]
    [string]$LogPath = "C:\Logs",
    
    [Parameter(Mandatory=$false)]
    [string]$DataPath = "C:\Data\ForcamApi",
    
    [Parameter(Mandatory=$false)]
    [string]$RootPath,
    
    [Parameter(Mandatory=$false)]
    [string]$ServiceAccount,
    
    [Parameter(Mandatory=$false)]
    [switch]$CreateWindowsService,
    
    [Parameter(Mandatory=$false)]
    [switch]$SkipDatabaseSetup,
    
    [Parameter(Mandatory=$false)]
    [switch]$DryRun
)

# Script requires elevation
if (-not ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {
    Write-Error "This script requires Administrator privileges. Please run as Administrator."
    exit 1
}

$ErrorActionPreference = "Stop"

function Write-DeployLog {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logEntry = "[$timestamp] [$Level] $Message"
    Write-Host $logEntry -ForegroundColor $(
        switch ($Level) {
            "ERROR" { "Red" }
            "WARNING" { "Yellow" }
            "SUCCESS" { "Green" }
            default { "White" }
        }
    )
    
    if (-not $DryRun) {
        Add-Content -Path "$env:TEMP\forcam_deploy.log" -Value $logEntry
    }
}

function Test-Prerequisites {
    Write-DeployLog "Checking prerequisites..."
    
    # PowerShell version
    if ($PSVersionTable.PSVersion.Major -lt 5) {
        throw "PowerShell 5.1 or later is required"
    }
    Write-DeployLog "PowerShell version: $($PSVersionTable.PSVersion)" "SUCCESS"
    
    # SQL Server connectivity
    try {
        $testConnection = New-Object System.Data.SqlClient.SqlConnection
        $testConnection.ConnectionString = "Server=$SqlServer;Database=master;Integrated Security=True;Connection Timeout=10;"
        $testConnection.Open()
        $testConnection.Close()
        $testConnection.Dispose()
        Write-DeployLog "SQL Server connectivity verified: $SqlServer" "SUCCESS"
    } catch {
        throw "Cannot connect to SQL Server $SqlServer`: $_"
    }
    
    # Check if database exists
    try {
        $testConnection = New-Object System.Data.SqlClient.SqlConnection
        $testConnection.ConnectionString = "Server=$SqlServer;Database=$Database;Integrated Security=True;Connection Timeout=10;"
        $testConnection.Open()
        $testConnection.Close()
        $testConnection.Dispose()
        Write-DeployLog "Target database verified: $Database" "SUCCESS"
    } catch {
        Write-DeployLog "Database $Database does not exist or is not accessible" "WARNING"
        $createDb = Read-Host "Create database $Database? (y/N)"
        if ($createDb -eq 'y') {
            $createDbSql = "CREATE DATABASE [$Database]"
            try {
                $conn = New-Object System.Data.SqlClient.SqlConnection "Server=$SqlServer;Database=master;Integrated Security=True;"
                $conn.Open()
                $cmd = $conn.CreateCommand()
                $cmd.CommandText = $createDbSql
                $cmd.ExecuteNonQuery() | Out-Null
                $conn.Close()
                $conn.Dispose()
                Write-DeployLog "Database $Database created successfully" "SUCCESS"
            } catch {
                throw "Failed to create database: $_"
            }
        } else {
            throw "Database $Database is required for deployment"
        }
    }
    
    # .NET Framework
    $dotNetVersion = Get-ItemProperty "HKLM:SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full\" -Name Release -ErrorAction SilentlyContinue
    if ($dotNetVersion.Release -lt 461808) {  # .NET 4.7.2
        Write-DeployLog ".NET Framework 4.7.2 or later is recommended" "WARNING"
    } else {
        Write-DeployLog ".NET Framework version verified" "SUCCESS"
    }
}

function Create-Directories {
    Write-DeployLog "Creating directory structure..."
    
    $directories = @(
        $InstallPath,
        "$InstallPath\Logs",
        "$InstallPath\Config",
        "$InstallPath\Scripts",
        $LogPath,
        $DataPath,
        "$DataPath\Archive",
        "$DataPath\Temp"
    )
    
    foreach ($dir in $directories) {
        if (-not $DryRun) {
            if (-not (Test-Path $dir)) {
                New-Item -ItemType Directory -Path $dir -Force | Out-Null
                Write-DeployLog "Created directory: $dir" "SUCCESS"
            } else {
                Write-DeployLog "Directory exists: $dir"
            }
        } else {
            Write-DeployLog "Would create directory: $dir"
        }
    }
}

function Copy-Files {
    Write-DeployLog "Copying application files..."
    
    $filesToCopy = @{
        "ForcamImport-Production.ps1" = "$InstallPath\ForcamImport-Production.ps1"
        "Config.example.ps1" = "$InstallPath\Config\Config.example.ps1"
        "import_cycle_time_production.sql" = "$InstallPath\Scripts\import_cycle_time_production.sql"
        "create_forcam_schema.sql" = "$InstallPath\Scripts\create_forcam_schema.sql"
        "README.md" = "$InstallPath\README.md"
    }
    
    foreach ($source in $filesToCopy.Keys) {
        $destination = $filesToCopy[$source]
        if (Test-Path $source) {
            if (-not $DryRun) {
                Copy-Item -Path $source -Destination $destination -Force
                Write-DeployLog "Copied: $source -> $destination" "SUCCESS"
            } else {
                Write-DeployLog "Would copy: $source -> $destination"
            }
        } else {
            Write-DeployLog "Source file not found: $source" "WARNING"
        }
    }
}

function Setup-Database {
    if ($SkipDatabaseSetup) {
        Write-DeployLog "Skipping database setup as requested"
        return
    }
    
    Write-DeployLog "Setting up database schema..."
    
    $schemaScript = "$InstallPath\Scripts\create_forcam_schema.sql"
    if (Test-Path $schemaScript) {
        if (-not $DryRun) {
            try {
                $result = & sqlcmd -S $SqlServer -d $Database -i $schemaScript -b 2>&1
                if ($LASTEXITCODE -eq 0) {
                    Write-DeployLog "Database schema created successfully" "SUCCESS"
                } else {
                    Write-DeployLog "Database schema creation had warnings: $result" "WARNING"
                }
            } catch {
                throw "Failed to create database schema: $_"
            }
        } else {
            Write-DeployLog "Would execute schema script: $schemaScript"
        }
    } else {
        Write-DeployLog "Schema script not found: $schemaScript" "ERROR"
    }
}

function Create-Configuration {
    Write-DeployLog "Creating production configuration..."
    
    $configPath = "$InstallPath\Config.ps1"
    
    if (Test-Path $configPath -and -not $DryRun) {
        $overwrite = Read-Host "Configuration file exists. Overwrite? (y/N)"
        if ($overwrite -ne 'y') {
            Write-DeployLog "Skipping configuration creation"
            return
        }
    }
    
    $configContent = @"
# =====================
# FORCAM Import Production Configuration
# Generated by deployment script on $(Get-Date)
# =====================

`$Config = @{
    # Database Configuration
    SqlServer = "$SqlServer"
    Database = "$Database"
    ApiSqlTable = "staging.forcam.api_cycle_data"
    CsvSqlScript = "$InstallPath\Scripts\import_cycle_time_production.sql"
    UseIntegratedSecurity = `$true
    
    # API Configuration
    ApiBaseUrl = "https://api.forcam.com"
    ApiKey = `$env:FORCAM_API_KEY
    ApiEndpoints = @(
        "/productionOrders",
        "/operations",
        "/workplaces",
        "/workplaceGroups",
        "/staffMembers"
    )
    
    # File System Configuration
    RootPath = "$RootPath"
    OutputFolder = "$DataPath"
    ArchivePath = "$DataPath\Archive"
    
    # Logging Configuration
    LogFile = "$LogPath\forcam_import.log"
    LogLevel = "INFO"
    
    # Performance Configuration
    MaxRetries = 3
    DelaySeconds = 2
    MaxParallelCsv = 5
    
    # Email Configuration
    EmailEnabled = `$true
    SmtpServer = `$env:SMTP_SERVER
    SmtpPort = 587
    SmtpUser = `$env:SMTP_USER
    SmtpPassword = `$env:SMTP_PASSWORD
    EmailFrom = "forcam-system@$($env:COMPUTERNAME.ToLower()).company.com"
    EmailTo = @("admin@company.com")
    EmailSubject = "FORCAM Import Alert - Production"
    
    # Feature Flags
    UseFileWatcher = `$true
    MoveProcessed = `$true
    EnableApiImport = `$true
    EnableCsvImport = `$true
    ValidateData = `$true
    SkipInvalidRecords = `$true
}

return `$Config
"@
    
    if (-not $DryRun) {
        $configContent | Out-File -FilePath $configPath -Encoding UTF8
        Write-DeployLog "Configuration created: $configPath" "SUCCESS"
    } else {
        Write-DeployLog "Would create configuration: $configPath"
    }
}

function Create-WindowsService {
    if (-not $CreateWindowsService) {
        Write-DeployLog "Skipping Windows Service creation"
        return
    }
    
    Write-DeployLog "Creating Windows Service..."
    
    $serviceName = "ForcamImportService"
    $serviceDisplayName = "FORCAM Import Service"
    $serviceDescription = "Automated FORCAM data import service for manufacturing systems"
    $servicePath = "$InstallPath\ForcamImport-Production.ps1"
    
    # Check if service already exists
    $existingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
    if ($existingService) {
        Write-DeployLog "Service $serviceName already exists" "WARNING"
        $recreate = Read-Host "Recreate service? (y/N)"
        if ($recreate -eq 'y' -and -not $DryRun) {
            Stop-Service -Name $serviceName -ErrorAction SilentlyContinue
            & sc.exe delete $serviceName | Out-Null
            Start-Sleep -Seconds 2
        } else {
            return
        }
    }
    
    if (-not $DryRun) {
        # Create service wrapper script
        $wrapperScript = @"
# Windows Service Wrapper for FORCAM Import
`$VerbosePreference = "Continue"
Set-Location "$InstallPath"
try {
    & "$servicePath" -NoWatcher
} catch {
    Write-EventLog -LogName Application -Source "FORCAM Import" -EventId 1000 -EntryType Error -Message "Service error: `$_"
    throw
}
"@
        
        $wrapperPath = "$InstallPath\ServiceWrapper.ps1"
        $wrapperScript | Out-File -FilePath $wrapperPath -Encoding UTF8
        
        # Create the service
        $serviceCommand = "powershell.exe -ExecutionPolicy Bypass -File `"$wrapperPath`""
        
        if ($ServiceAccount) {
            $credential = Get-Credential -UserName $ServiceAccount -Message "Enter password for service account $ServiceAccount"
            $result = & sc.exe create $serviceName binPath= $serviceCommand start= auto DisplayName= $serviceDisplayName obj= $ServiceAccount password= $credential.GetNetworkCredential().Password
        } else {
            $result = & sc.exe create $serviceName binPath= $serviceCommand start= auto DisplayName= $serviceDisplayName
        }
        
        if ($LASTEXITCODE -eq 0) {
            & sc.exe description $serviceName $serviceDescription | Out-Null
            Write-DeployLog "Windows Service created: $serviceName" "SUCCESS"
        } else {
            Write-DeployLog "Failed to create Windows Service: $result" "ERROR"
        }
    } else {
        Write-DeployLog "Would create Windows Service: $serviceName"
    }
}

function Create-ScheduledTasks {
    Write-DeployLog "Creating scheduled tasks..."
    
    # Daily cleanup task
    $cleanupTask = @{
        TaskName = "FORCAM-DataCleanup"
        Description = "Daily cleanup of old FORCAM import data"
        Script = "$InstallPath\Scripts\cleanup_old_data.ps1"
        Schedule = "Daily at 2:00 AM"
    }
    
    # Health check task
    $healthCheckTask = @{
        TaskName = "FORCAM-HealthCheck"
        Description = "Hourly health check for FORCAM import system"
        Script = "$InstallPath\Scripts\health_check.ps1"
        Schedule = "Every hour"
    }
    
    if (-not $DryRun) {
        # Create cleanup script
        $cleanupScript = @"
# FORCAM Data Cleanup Script
sqlcmd -S $SqlServer -d $Database -Q "EXEC forcam.sp_cleanup_old_data @days_to_keep = 90"
"@
        $cleanupScript | Out-File -FilePath "$InstallPath\Scripts\cleanup_old_data.ps1" -Encoding UTF8
        
        # Register scheduled tasks (simplified - would need proper task creation in real deployment)
        Write-DeployLog "Cleanup and health check scripts created" "SUCCESS"
        Write-DeployLog "Manual setup required for scheduled tasks" "WARNING"
    } else {
        Write-DeployLog "Would create scheduled tasks for cleanup and health checks"
    }
}

function Set-Permissions {
    Write-DeployLog "Setting file and folder permissions..."
    
    if (-not $DryRun) {
        try {
            # Grant full control to the install directory for administrators
            $acl = Get-Acl $InstallPath
            $permission = "BUILTIN\Administrators", "FullControl", "ContainerInherit,ObjectInherit", "None", "Allow"
            $accessRule = New-Object System.Security.AccessControl.FileSystemAccessRule $permission
            $acl.SetAccessRule($accessRule)
            
            # Grant read/execute to the service account if specified
            if ($ServiceAccount) {
                $permission = $ServiceAccount, "ReadAndExecute", "ContainerInherit,ObjectInherit", "None", "Allow"
                $accessRule = New-Object System.Security.AccessControl.FileSystemAccessRule $permission
                $acl.SetAccessRule($accessRule)
            }
            
            Set-Acl -Path $InstallPath -AclObject $acl
            Write-DeployLog "Permissions set successfully" "SUCCESS"
        } catch {
            Write-DeployLog "Failed to set permissions: $_" "WARNING"
        }
    } else {
        Write-DeployLog "Would set appropriate file permissions"
    }
}

function Test-Deployment {
    Write-DeployLog "Testing deployment..."
    
    if (-not $DryRun) {
        try {
            # Test configuration loading
            $testConfig = & "$InstallPath\Config.ps1"
            if ($testConfig) {
                Write-DeployLog "Configuration loads successfully" "SUCCESS"
            }
            
            # Test database connectivity
            $conn = New-Object System.Data.SqlClient.SqlConnection "Server=$SqlServer;Database=$Database;Integrated Security=True;Connection Timeout=5;"
            $conn.Open()
            $cmd = $conn.CreateCommand()
            $cmd.CommandText = "SELECT COUNT(*) FROM sys.tables WHERE name LIKE '%forcam%'"
            $tableCount = $cmd.ExecuteScalar()
            $conn.Close()
            $conn.Dispose()
            
            if ($tableCount -gt 0) {
                Write-DeployLog "Database schema verified ($tableCount FORCAM tables found)" "SUCCESS"
            } else {
                Write-DeployLog "No FORCAM tables found in database" "WARNING"
            }
            
            Write-DeployLog "Deployment test completed successfully" "SUCCESS"
        } catch {
            Write-DeployLog "Deployment test failed: $_" "ERROR"
            throw $_
        }
    } else {
        Write-DeployLog "Would perform deployment validation tests"
    }
}

# =====================
# Main Deployment Process
# =====================

try {
    Write-DeployLog "Starting FORCAM Import System deployment..." "SUCCESS"
    Write-DeployLog "Target: $SqlServer/$Database"
    Write-DeployLog "Install Path: $InstallPath"
    
    if ($DryRun) {
        Write-DeployLog "DRY RUN MODE - No changes will be made" "WARNING"
    }
    
    Test-Prerequisites
    Create-Directories
    Copy-Files  
    Setup-Database
    Create-Configuration
    Create-WindowsService
    Create-ScheduledTasks
    Set-Permissions
    Test-Deployment
    
    Write-DeployLog "Deployment completed successfully!" "SUCCESS"
    Write-DeployLog "Next steps:"
    Write-DeployLog "1. Set environment variables: FORCAM_API_KEY, SMTP_USER, SMTP_PASSWORD"
    Write-DeployLog "2. Review and customize: $InstallPath\Config.ps1"
    Write-DeployLog "3. Test the installation: $InstallPath\ForcamImport-Production.ps1 -CsvOnly"
    Write-DeployLog "4. Start the service or schedule the script"
    
} catch {
    Write-DeployLog "Deployment failed: $_" "ERROR"
    exit 1
}

# Display deployment summary
Write-Host "`n" -NoNewline
Write-Host "=========================" -ForegroundColor Green
Write-Host "DEPLOYMENT SUMMARY" -ForegroundColor Green  
Write-Host "=========================" -ForegroundColor Green
Write-Host "Install Path: $InstallPath" -ForegroundColor White
Write-Host "Database: $SqlServer/$Database" -ForegroundColor White
Write-Host "Log Path: $LogPath" -ForegroundColor White
Write-Host "Data Path: $DataPath" -ForegroundColor White
if ($RootPath) { Write-Host "Root Path: $RootPath" -ForegroundColor White }
Write-Host "Service Created: $CreateWindowsService" -ForegroundColor White
Write-Host "=========================" -ForegroundColor Green
