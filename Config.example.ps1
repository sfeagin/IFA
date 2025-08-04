# =====================
# FORCAM API + CSV Import Configuration - EXAMPLE
# Copy this file to Config.ps1 and customize for your environment
# =====================

# DO NOT COMMIT THE ACTUAL Config.ps1 FILE TO VERSION CONTROL
# IT CONTAINS SENSITIVE INFORMATION

$Config = @{
    # =====================
    # API Configuration
    # =====================
    ApiBaseUrl = "https://your-forcam-api.company.com/api/v1"
    ApiKey = $env:FORCAM_API_KEY  # Set as environment variable: $env:FORCAM_API_KEY = "your-key"
    ApiEndpoints = @(
        "/productionOrders",
        "/operations", 
        "/workplaces",
        "/workplaceGroups",
        "/staffMembers",
        "/callbacks",
        "/tools",
        "/materials"
    )
    
    # =====================
    # Database Configuration
    # =====================
    SqlServer = $env:FORCAM_SQL_SERVER -ne $null ? $env:FORCAM_SQL_SERVER : "sql-server.company.com"
    Database = "staging"
    ApiSqlTable = "staging.forcam.api_cycle_data"
    CsvSqlScript = "C:\Scripts\ForcamImport\import_cycle_time_production.sql"
    
    # Authentication - choose one method
    UseIntegratedSecurity = $true    # Windows Authentication (recommended)
    SqlUser = $env:SQL_USER          # SQL Server Authentication (if not using Windows Auth)
    SqlPassword = $env:SQL_PASSWORD  # SQL Server Authentication (if not using Windows Auth)
    
    # Connection settings
    ConnectionTimeout = 30           # Seconds to wait for connection
    CommandTimeout = 600            # Seconds to wait for SQL commands
    
    # =====================
    # File System Configuration
    # =====================
    RootPath = "\\fileserver.company.com\ForcamData\Production"  # Main data directory
    OutputFolder = "C:\Data\ForcamApi"                           # Local output for API data
    ArchivePath = "C:\Data\ForcamApi\Archive"                   # Archive location
    
    # Directory structure expected:
    # RootPath\
    #   ├── Machine001\
    #   │   ├── *.csv files
    #   │   ├── Backup\
    #   │   └── Error\
    #   ├── Machine002\
    #   └── etc...
    
    # =====================
    # Performance & Reliability Configuration
    # =====================
    MaxRetries = 3                   # Number of retry attempts for failed operations
    DelaySeconds = 2                 # Base delay between retries (with exponential backoff)
    MaxParallelCsv = 5              # Maximum parallel CSV processing jobs
    FileUnlockRetries = 6           # Attempts to wait for file unlock
    FileUnlockWait = 5              # Seconds to wait between file unlock attempts
    BatchSize = 1000                # Records per batch for bulk operations
    TransactionTimeout = 300        # Transaction timeout in seconds
    
    # =====================
    # Logging Configuration
    # =====================
    LogFile = "C:\Logs\forcam_import.log"
    LogLevel = "INFO"               # DEBUG, INFO, WARNING, ERROR, FATAL
    
    # Log rotation (handled externally or via scheduled task)
    MaxLogSizeMB = 100             # Maximum log file size before rotation
    MaxLogFiles = 10               # Number of log files to keep
    
    # =====================
    # Email Alert Configuration
    # =====================
    EmailEnabled = $true
    SmtpServer = $env:SMTP_SERVER -ne $null ? $env:SMTP_SERVER : "smtp.company.com"
    SmtpPort = 587
    SmtpUser = $env:SMTP_USER      # Set as environment variable
    SmtpPassword = $env:SMTP_PASSWORD  # Set as environment variable
    EmailFrom = "forcam-system@company.com"
    EmailTo = @(
        "manufacturing-ops@company.com",
        "it-systems@company.com",
        "data-team@company.com"
    )
    EmailSubject = "FORCAM Import Alert - Production"
    
    # Different alert levels can have different recipients
    EmailCritical = @("oncall@company.com", "manager@company.com")
    
    # =====================
    # Feature Flags
    # =====================
    UseFileWatcher = $true          # Enable real-time file monitoring
    MoveProcessed = $true           # Move successfully processed files to Backup
    EnableApiImport = $true         # Enable API data import
    EnableCsvImport = $true         # Enable CSV file import
    EnablePerformanceCounters = $true  # Track performance metrics
    
    # =====================
    # Data Processing Configuration
    # =====================
    ValidateData = $true            # Enable data validation
    SkipInvalidRecords = $true      # Skip invalid records instead of failing
    MaxErrorsPerBatch = 10          # Maximum errors allowed per batch before failing
    
    # Data transformation settings
    RoundCycleTimes = $true         # Round cycle times to 6 decimal places
    TimestampFormat = "yyyy-MM-dd HH:mm:ss"  # Expected timestamp format
    
    # =====================
    # Monitoring & Alerting Thresholds
    # =====================
    MaxApiResponseTimeMs = 5000     # Alert if API response time exceeds this
    MaxCsvProcessingTimeMs = 30000  # Alert if CSV processing time exceeds this
    MaxErrorRatePercent = 5         # Alert if error rate exceeds this percentage
    MinRecordsPerHour = 100        # Alert if hourly record count falls below this
    
    # =====================
    # Security Configuration
    # =====================
    ApiTimeout = 30                 # API request timeout in seconds
    MaxApiRequestsPerMinute = 60    # Rate limiting for API calls
    RequireHttps = $true           # Force HTTPS for API calls
    ValidateCertificates = $true    # Validate SSL certificates
    
    # File access security
    RequireFileReadPermission = $true
    ValidateFileIntegrity = $false  # Enable checksum validation (if available)
    
    # =====================
    # Maintenance Configuration
    # =====================
    DataRetentionDays = 90         # Days to keep detailed data
    ErrorLogRetentionDays = 365    # Days to keep error logs
    PerformanceDataDays = 30       # Days to keep performance metrics
    
    # Cleanup schedule (run via scheduled task)
    EnableAutoCleanup = $true
    CleanupHour = 2                # Hour to run cleanup (24-hour format)
    
    # =====================
    # Development/Testing Overrides
    # =====================
    # These will be applied based on the ENVIRONMENT variable
}

# =====================
# Environment-specific Configuration
# =====================
switch ($env:ENVIRONMENT) {
    "Development" {
        Write-Host "Loading Development configuration..." -ForegroundColor Yellow
        $Config.SqlServer = "dev-sql-server.company.com"
        $Config.Database = "staging_dev"
        $Config.RootPath = "C:\Dev\ForcamData"
        $Config.OutputFolder = "C:\Dev\ForcamApi"
        $Config.LogLevel = "DEBUG"
        $Config.EmailEnabled = $false
        $Config.MaxRetries = 2
        $Config.DelaySeconds = 1
        $Config.MaxParallelCsv = 2
        $Config.ApiBaseUrl = "https://dev-api.forcam.company.com/api/v1"
    }
    
    "Testing" {
        Write-Host "Loading Test configuration..." -ForegroundColor Cyan
        $Config.SqlServer = "test-sql-server.company.com"
        $Config.Database = "staging_test"
        $Config.RootPath = "\\test-fileserver\ForcamData"
        $Config.LogLevel = "INFO"
        $Config.EmailTo = @("test-team@company.com")
        $Config.MaxRetries = 2
        $Config.ApiBaseUrl = "https://test-api.forcam.company.com/api/v1"
    }
    
    "Production" {
        Write-Host "Loading Production configuration..." -ForegroundColor Green
        # Production settings are the defaults above
        $Config.EnablePerformanceCounters = $true
        $Config.ValidateData = $true
        $Config.EmailEnabled = $true
    }
    
    default {
        Write-Host "No environment specified, using Production defaults..." -ForegroundColor White
        $env:ENVIRONMENT = "Production"
    }
}

# =====================
# Configuration Validation
# =====================
$requiredEnvVars = @()

if ($Config.EnableApiImport) {
    $requiredEnvVars += "FORCAM_API_KEY"
}

if ($Config.EmailEnabled) {
    $requiredEnvVars += @("SMTP_USER", "SMTP_PASSWORD")
}

if (-not $Config.UseIntegratedSecurity) {
    $requiredEnvVars += @("SQL_USER", "SQL_PASSWORD")
}

# Validate required environment variables
foreach ($envVar in $requiredEnvVars) {
    if (-not (Get-Item "env:$envVar" -ErrorAction SilentlyContinue)) {
        throw "Required environment variable $envVar is not set. Please configure it before running the script."
    }
}

# Validate paths exist or can be created
$pathsToValidate = @(
    (Split-Path $Config.LogFile -Parent),
    $Config.OutputFolder,
    $Config.ArchivePath
)

foreach ($path in $pathsToValidate) {
    if (-not (Test-Path $path)) {
        try {
            New-Item -ItemType Directory -Path $path -Force | Out-Null
            Write-Host "Created directory: $path" -ForegroundColor Green
        } catch {
            Write-Warning "Could not create directory: $path - $_"
        }
    }
}

# Validate network paths accessibility (non-blocking)
if ($Config.RootPath.StartsWith("\\")) {
    if (-not (Test-Path $Config.RootPath -ErrorAction SilentlyContinue)) {
        Write-Warning "Root path may not be accessible: $($Config.RootPath)"
        Write-Warning "This may cause issues during CSV import."
    }
}

# Final configuration summary
Write-Host "Configuration loaded successfully:" -ForegroundColor Green
Write-Host "  Environment: $($env:ENVIRONMENT)" -ForegroundColor White
Write-Host "  Database: $($Config.SqlServer)/$($Config.Database)" -ForegroundColor White
Write-Host "  API Import: $($Config.EnableApiImport)" -ForegroundColor White
Write-Host "  CSV Import: $($Config.EnableCsvImport)" -ForegroundColor White
Write-Host "  Email Alerts: $($Config.EmailEnabled)" -ForegroundColor White
Write-Host "  Log Level: $($Config.LogLevel)" -ForegroundColor White

# Return the configuration
return $Config
