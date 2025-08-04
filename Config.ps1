# =====================
# FORCAM API + CSV Import Configuration
# Production Environment Settings
# =====================

$Config = @{
    # API Configuration
    ApiBaseUrl = "https://api.forcam.com"
    ApiKey = $env:FORCAM_API_KEY  # Set as environment variable for security
    ApiEndpoints = @(
        "/productionOrders",
        "/operations", 
        "/workplaces",
        "/workplaceGroups",
        "/staffMembers",
        "/callbacks"
    )
    
    # Database Configuration
    SqlServer = $env:FORCAM_SQL_SERVER -ne $null ? $env:FORCAM_SQL_SERVER : "localhost"
    Database = "staging"
    ApiSqlTable = "staging.forcam.api_cycle_data"
    CsvSqlScript = "C:\Scripts\import_cycle_time.sql"
    
    # File System Configuration
    RootPath = "\\SUSA059A59\Output_Prod"
    OutputFolder = "C:\Data\ForcamApi"
    ArchivePath = "C:\Data\ForcamApi\Archive"
    
    # Retry and Performance Configuration
    MaxRetries = 3
    DelaySeconds = 2
    MaxParallelCsv = 5
    FileUnlockRetries = 6
    FileUnlockWait = 5
    
    # Logging Configuration
    LogFile = "C:\Logs\forcam_import.log"
    LogLevel = "INFO"  # DEBUG, INFO, WARNING, ERROR, FATAL
    
    # Email Alert Configuration
    EmailEnabled = $true
    SmtpServer = $env:SMTP_SERVER -ne $null ? $env:SMTP_SERVER : "smtp.company.com"
    SmtpPort = 587
    SmtpUser = $env:SMTP_USER
    SmtpPassword = $env:SMTP_PASSWORD
    EmailFrom = "forcam-system@company.com"
    EmailTo = @("admin@company.com", "operations@company.com")
    EmailSubject = "FORCAM Import Alert - Production"
    
    # Feature Flags
    UseFileWatcher = $true
    MoveProcessed = $true
    EnableApiImport = $true
    EnableCsvImport = $true
    
    # Security Configuration
    UseIntegratedSecurity = $true
    ConnectionTimeout = 30
    CommandTimeout = 600
    
    # Data Validation Configuration
    ValidateData = $true
    SkipInvalidRecords = $true
    MaxErrorsPerBatch = 10
    
    # Performance Monitoring
    EnablePerformanceCounters = $true
    BatchSize = 1000
    TransactionTimeout = 300
}

# Environment-specific overrides
switch ($env:ENVIRONMENT) {
    "Development" {
        $Config.SqlServer = "dev-sql-server"
        $Config.Database = "staging_dev"
        $Config.RootPath = "C:\Dev\ForcamData"
        $Config.LogLevel = "DEBUG"
        $Config.EmailEnabled = $false
    }
    "Testing" {
        $Config.SqlServer = "test-sql-server"
        $Config.Database = "staging_test"
        $Config.RootPath = "C:\Test\ForcamData"
        $Config.MaxRetries = 2
        $Config.EmailTo = @("test@company.com")
    }
    "Production" {
        # Production settings are the defaults above
        $Config.EnablePerformanceCounters = $true
        $Config.ValidateData = $true
    }
}

# Validate required environment variables
$requiredEnvVars = @("FORCAM_API_KEY")
if ($Config.EmailEnabled) {
    $requiredEnvVars += @("SMTP_USER", "SMTP_PASSWORD")
}

foreach ($envVar in $requiredEnvVars) {
    if (-not (Get-Item "env:$envVar" -ErrorAction SilentlyContinue)) {
        throw "Required environment variable $envVar is not set"
    }
}

# Create required directories
$directories = @(
    (Split-Path $Config.LogFile -Parent),
    $Config.OutputFolder,
    $Config.ArchivePath
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "Created directory: $dir"
    }
}

# Export configuration
return $Config
