# FORCAM API + CSV Import System

A robust, production-grade PowerShell solution for importing FORCAM manufacturing data from APIs and CSV files into SQL Server.

## Overview

This system provides automated data integration capabilities for FORCAM manufacturing execution systems, supporting both real-time API data ingestion and batch CSV file processing with comprehensive error handling, monitoring, and reporting.

## Features

### Core Functionality
- **API Data Import**: Automated polling of FORCAM API endpoints with pagination support
- **CSV File Processing**: Batch processing of CSV files from network locations
- **Real-time Monitoring**: File system watcher for automatic processing of new files
- **Data Validation**: Comprehensive validation and transformation of incoming data
- **Error Handling**: Robust retry mechanisms and error logging

### Production Features
- **Performance Monitoring**: Built-in performance counters and metrics
- **Email Alerting**: Configurable email notifications for errors and status updates
- **Parallel Processing**: Multi-threaded CSV processing for improved throughput
- **Transaction Safety**: All database operations wrapped in transactions
- **Data Deduplication**: MERGE operations prevent duplicate records

### Database Features
- **Comprehensive Schema**: Complete table structure with proper indexing
- **Summary Views**: Pre-built views for different time aggregations
- **Data Quality Checks**: Built-in procedures for data validation
- **Automated Cleanup**: Configurable data retention policies

## Quick Start

### Prerequisites
- Windows Server 2016+ or Windows 10+
- PowerShell 5.1 or later
- SQL Server 2016+ with appropriate permissions
- Network access to FORCAM API endpoints
- Access to CSV file locations

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd forcam-import-system
   ```

2. **Set up the database schema**
   ```sql
   sqlcmd -S YourSqlServer -d staging -i create_forcam_schema.sql
   ```

3. **Configure environment variables**
   ```powershell
   $env:FORCAM_API_KEY = "your-api-key-here"
   $env:FORCAM_SQL_SERVER = "your-sql-server"
   $env:SMTP_USER = "smtp-username"
   $env:SMTP_PASSWORD = "smtp-password"
   $env:ENVIRONMENT = "Production"  # or Development, Testing
   ```

4. **Run the system**
   ```powershell
   .\ForcamImport-Production.ps1
   ```

## Configuration

The system uses a centralized configuration file (`Config.ps1`) with environment-specific overrides:

### Key Configuration Options

```powershell
$Config = @{
    # API Settings
    ApiBaseUrl = "https://api.forcam.com"
    ApiEndpoints = @("/productionOrders", "/operations", "/workplaces")
    
    # Database Settings
    SqlServer = "your-sql-server"
    Database = "staging"
    
    # File Processing
    RootPath = "\\server\share\ForcamData"
    MaxParallelCsv = 5
    
    # Performance & Reliability
    MaxRetries = 3
    DelaySeconds = 2
    
    # Monitoring
    EmailEnabled = $true
    LogLevel = "INFO"
}
```

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `FORCAM_API_KEY` | API authentication key | Yes (if API enabled) |
| `FORCAM_SQL_SERVER` | SQL Server instance | No (defaults to localhost) |
| `SMTP_USER` | Email username | Yes (if email enabled) |
| `SMTP_PASSWORD` | Email password | Yes (if email enabled) |
| `ENVIRONMENT` | Deployment environment | No (defaults to Production) |

## Usage

### Command Line Options

```powershell
# Run full import (API + CSV)
.\ForcamImport-Production.ps1

# API import only
.\ForcamImport-Production.ps1 -ApiOnly

# CSV import only
.\ForcamImport-Production.ps1 -CsvOnly

# Disable file watcher
.\ForcamImport-Production.ps1 -NoWatcher

# Enable debug logging
.\ForcamImport-Production.ps1 -LogLevel DEBUG
```

### File Structure

The system expects the following directory structure:

```
RootPath/
├── Machine1/
│   ├── data1.csv
│   ├── data2.csv
│   ├── Backup/
│   └── Error/
├── Machine2/
│   ├── data3.csv
│   ├── Backup/
│   └── Error/
└── ...
```

### CSV File Format

CSV files should follow this structure:

```csv
date,time,workplace,ordernumber,operationnumber,materialnumber,te_sap
2025-01-01,08:00:00,WP001,ORD123,OP456,MAT789,45.5
2025-01-01,08:01:00,WP001,ORD123,OP456,MAT789,46.2
...
```

## Database Schema

### Main Tables

#### `staging.forcam.cycle_time`
Primary table for CSV data with cycle time information.

#### `staging.forcam.api_cycle_data`
Storage for API-sourced manufacturing data.

#### `staging.forcam.error_log`
Comprehensive error tracking and debugging information.

#### `staging.forcam.import_stats`
Performance metrics and import statistics.

### Summary Views

The system provides pre-built views for different time aggregations:

- `forcam.vw_minute_summary` - Minute-level aggregations
- `forcam.vw_hourly_summary` - Hourly summaries
- `forcam.vw_daily_summary` - Daily reports
- `forcam.vw_weekly_summary` - Weekly analysis (ISO weeks)
- `forcam.vw_monthly_summary` - Monthly trends
- `forcam.vw_quarterly_summary` - Quarterly reports

### Example Queries

```sql
-- Daily machine performance
SELECT * FROM forcam.vw_daily_summary
WHERE day_block = '2025-01-15'
ORDER BY machine_name;

-- Error summary for last 7 days
SELECT * FROM forcam.vw_error_summary
WHERE error_date >= DATEADD(DAY, -7, GETDATE());

-- Import status
SELECT * FROM forcam.vw_import_status
WHERE import_date >= CAST(GETDATE() AS DATE)
ORDER BY import_date DESC;
```

## Monitoring and Alerting

### Logging

The system provides comprehensive logging with configurable levels:

- **DEBUG**: Detailed execution information
- **INFO**: General operational messages
- **WARNING**: Non-critical issues
- **ERROR**: Failed operations
- **FATAL**: Critical system failures

### Email Alerts

Automatic email notifications for:
- System startup/shutdown
- Import failures
- Data quality issues
- Performance degradation

### Performance Monitoring

Built-in counters track:
- API requests per second
- CSV files processed per minute
- Records imported per second
- Average response times
- Error rates

## Maintenance

### Data Cleanup

Use the built-in cleanup procedure:

```sql
-- Clean data older than 90 days
EXEC forcam.sp_cleanup_old_data @days_to_keep = 90;
```

### Data Quality Checks

Regular quality validation:

```sql
-- Check data quality for a specific machine
EXEC forcam.sp_data_quality_check @machine_name = 'Machine1';

-- Check today's data across all machines
EXEC forcam.sp_data_quality_check;
```

### Log Management

Configure log rotation in your environment or use the built-in log cleanup:

```powershell
# Archive logs older than 30 days
Get-ChildItem "C:\Logs\*.log" | 
Where-Object {$_.LastWriteTime -lt (Get-Date).AddDays(-30)} |
ForEach-Object {
    Compress-Archive -Path $_.FullName -DestinationPath "C:\Logs\Archive\$($_.BaseName)_$(Get-Date -Format 'yyyyMMdd').zip"
    Remove-Item $_.FullName
}
```

## Troubleshooting

### Common Issues

#### API Connection Problems
- Verify API key validity
- Check network connectivity
- Review firewall settings
- Validate SSL certificates

#### Database Connection Issues
- Confirm SQL Server accessibility
- Verify authentication method
- Check database permissions
- Review connection string format

#### File Processing Failures
- Validate file format and encoding
- Check file permissions
- Verify network path accessibility
- Review CSV structure

### Debug Mode

Enable detailed logging for troubleshooting:

```powershell
.\ForcamImport-Production.ps1 -LogLevel DEBUG
```

### Error Investigation

Query the error log for specific issues:

```sql
-- Recent errors by machine
SELECT TOP 50 * FROM staging.forcam.error_log
WHERE machine_name = 'YourMachine'
ORDER BY error_date DESC;

-- Error patterns
SELECT 
    LEFT(error_message, 100) AS error_pattern,
    COUNT(*) AS occurrence_count,
    MIN(error_date) AS first_seen,
    MAX(error_date) AS last_seen
FROM staging.forcam.error_log
WHERE error_date >= DATEADD(DAY, -7, GETDATE())
GROUP BY LEFT(error_message, 100)
ORDER BY occurrence_count DESC;
```

## Security Considerations

### Credentials Management
- Use Windows environment variables for sensitive data
- Never commit credentials to version control
- Implement credential rotation policies
- Use Windows Authentication where possible

### Network Security
- Implement SSL/TLS for API communications
- Use VPN for remote database connections
- Restrict file share access permissions
- Monitor network traffic for anomalies

### Database Security
- Use least-privilege database accounts
- Implement connection encryption
- Regular security audits
- Backup encryption

## Performance Tuning

### Database Optimization
- Regularly update statistics
- Monitor index fragmentation
- Partition large tables by date
- Implement appropriate retention policies

### PowerShell Performance
- Adjust parallel processing limits
- Monitor memory usage
- Optimize batch sizes
- Use connection pooling

### Network Optimization
- Implement local caching where appropriate
- Use compression for large data transfers
- Monitor bandwidth utilization
- Implement retry with exponential backoff

## Support and Maintenance

### Regular Tasks
- Monitor system performance daily
- Review error logs weekly
- Validate data quality monthly
- Update API credentials quarterly
- Review and update configuration annually

### Health Checks
- Database connectivity
- API endpoint availability  
- File system accessibility
- Email notification functionality
- Performance metric trends

## License

This project is proprietary software. See LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Update documentation
5. Submit a pull request

## Changelog

### Version 2.0 (2025-08-04)
- Complete rewrite for production deployment
- Enhanced error handling and retry logic
- Comprehensive monitoring and alerting
- Performance optimizations
- Improved database schema
- Added data quality checks
- Enhanced security features

### Version 1.0 (Previous)
- Initial implementation
- Basic API and CSV import functionality
- Simple error handling
