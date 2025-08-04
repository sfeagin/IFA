-- =====================
-- FORCAM Database Schema Creation Script
-- Version: 2.0
-- Description: Complete schema setup for FORCAM API and CSV import system
-- =====================

USE [staging];
GO

-- Create schemas if they don't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA [staging]');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'forcam')
    EXEC('CREATE SCHEMA [forcam]');
GO

PRINT 'Schemas created/verified successfully';

-- =====================
-- Main Data Tables
-- =====================

-- API Cycle Data Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.api_cycle_data') AND type = 'U')
BEGIN
    CREATE TABLE staging.forcam.api_cycle_data (
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
        
        -- Indexes for performance
        INDEX IX_ApiData_MachineName_Timestamp (machine_name, timestamp),
        INDEX IX_ApiData_MaterialNumber (material_number),
        INDEX IX_ApiData_ImportTimestamp (import_timestamp),
        INDEX IX_ApiData_BatchId (batch_id)
    );
    
    PRINT 'Created table: staging.forcam.api_cycle_data';
END
ELSE
BEGIN
    PRINT 'Table staging.forcam.api_cycle_data already exists';
END
GO

-- CSV Cycle Time Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.cycle_time') AND type = 'U')
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
        machine_cycle_time AS CAST(te_sap AS DECIMAL(18,6)) PERSISTED,
        import_timestamp DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        source_file NVARCHAR(500),
        batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        
        -- Composite unique constraint to prevent duplicates
        CONSTRAINT UK_CycleTime_Unique UNIQUE (machine_name, [date], [time], materialnumber, operationnumber),
        
        -- Indexes for performance
        INDEX IX_CycleTime_MachineName_Date (machine_name, [date]),
        INDEX IX_CycleTime_MaterialNumber (materialnumber),
        INDEX IX_CycleTime_ImportTimestamp (import_timestamp),
        INDEX IX_CycleTime_BatchId (batch_id),
        INDEX IX_CycleTime_DateTime ([date], [time])
    );
    
    PRINT 'Created table: staging.forcam.cycle_time';
END
ELSE
BEGIN
    PRINT 'Table staging.forcam.cycle_time already exists';
END
GO

-- Error Log Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.error_log') AND type = 'U')
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
        error_context NVARCHAR(MAX),
        
        -- Indexes for querying
        INDEX IX_ErrorLog_ErrorDate (error_date),
        INDEX IX_ErrorLog_MachineName (machine_name),
        INDEX IX_ErrorLog_BatchId (batch_id)
    );
    
    PRINT 'Created table: staging.forcam.error_log';
END
ELSE
BEGIN
    PRINT 'Table staging.forcam.error_log already exists';
END
GO

-- Import Statistics Table
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'staging.forcam.import_stats') AND type = 'U')
BEGIN
    CREATE TABLE staging.forcam.import_stats (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        import_date DATETIME2(3) NOT NULL DEFAULT GETDATE(),
        import_type NVARCHAR(20) NOT NULL, -- 'API' or 'CSV'
        machine_name NVARCHAR(100),
        source_path NVARCHAR(500),
        records_processed INT NOT NULL DEFAULT 0,
        records_inserted INT NOT NULL DEFAULT 0,
        records_updated INT NOT NULL DEFAULT 0,
        records_skipped INT NOT NULL DEFAULT 0,
        duration_ms INT NOT NULL DEFAULT 0,
        batch_id UNIQUEIDENTIFIER,
        status NVARCHAR(20) NOT NULL DEFAULT 'SUCCESS', -- SUCCESS, FAILED, PARTIAL
        
        INDEX IX_ImportStats_ImportDate (import_date),
        INDEX IX_ImportStats_ImportType (import_type),
        INDEX IX_ImportStats_MachineName (machine_name),
        INDEX IX_ImportStats_BatchId (batch_id)
    );
    
    PRINT 'Created table: staging.forcam.import_stats';
END
ELSE
BEGIN
    PRINT 'Table staging.forcam.import_stats already exists';
END
GO

-- =====================
-- Summary Views
-- =====================

-- Minute-Level Summary View
CREATE OR ALTER VIEW forcam.vw_minute_summary AS
SELECT
    machine_name,
    DATEADD(MINUTE, DATEDIFF(MINUTE, 0, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)), 0) AS minute_block,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    DATEADD(MINUTE, DATEDIFF(MINUTE, 0, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)), 0);
GO

-- Hourly Summary View
CREATE OR ALTER VIEW forcam.vw_hourly_summary AS
SELECT
    machine_name,
    DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)), 0) AS hour_block,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    DATEADD(HOUR, DATEDIFF(HOUR, 0, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)), 0);
GO

-- Daily Summary View
CREATE OR ALTER VIEW forcam.vw_daily_summary AS
SELECT
    machine_name,
    [date] AS day_block,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations,
    COUNT(DISTINCT workplace) AS unique_workplaces
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    [date];
GO

-- Weekly Summary View (ISO Week)
CREATE OR ALTER VIEW forcam.vw_weekly_summary AS
SELECT
    machine_name,
    DATEPART(YEAR, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS year,
    DATEPART(ISO_WEEK, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS iso_week,
    MIN([date]) AS week_start,
    MAX([date]) AS week_end,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    DATEPART(YEAR, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)),
    DATEPART(ISO_WEEK, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2));
GO

-- Monthly Summary View
CREATE OR ALTER VIEW forcam.vw_monthly_summary AS
SELECT
    machine_name,
    FORMAT(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2), 'yyyy-MM') AS month_block,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    FORMAT(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2), 'yyyy-MM');
GO

-- Quarterly Summary View
CREATE OR ALTER VIEW forcam.vw_quarterly_summary AS
SELECT
    machine_name,
    DATEPART(YEAR, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS year,
    DATEPART(QUARTER, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS quarter,
    COUNT(*) AS total_cycles,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS first_entry,
    MAX(CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)) AS last_entry,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY
    machine_name,
    DATEPART(YEAR, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2)),
    DATEPART(QUARTER, CAST(CONCAT([date], ' ', CONVERT(VARCHAR(8), [time], 108)) AS DATETIME2));
GO

-- =====================
-- Utility Views
-- =====================

-- Import Status View
CREATE OR ALTER VIEW forcam.vw_import_status AS
SELECT
    import_date,
    import_type,
    machine_name,
    source_path,
    records_processed,
    records_inserted,
    records_updated,
    records_skipped,
    duration_ms,
    status,
    CAST(records_inserted AS FLOAT) / NULLIF(records_processed, 0) * 100 AS success_rate_pct,
    batch_id
FROM staging.forcam.import_stats;
GO

-- Error Summary View
CREATE OR ALTER VIEW forcam.vw_error_summary AS
SELECT
    CAST(error_date AS DATE) AS error_date,
    COUNT(*) AS error_count,
    COUNT(DISTINCT machine_name) AS affected_machines,
    COUNT(DISTINCT batch_id) AS affected_batches,
    STRING_AGG(DISTINCT LEFT(error_message, 100), '; ') AS sample_errors
FROM staging.forcam.error_log
GROUP BY CAST(error_date AS DATE);
GO

-- Machine Performance View
CREATE OR ALTER VIEW forcam.vw_machine_performance AS
SELECT
    machine_name,
    COUNT(*) AS total_records,
    AVG(machine_cycle_time) AS avg_cycle_time,
    MIN(machine_cycle_time) AS min_cycle_time,
    MAX(machine_cycle_time) AS max_cycle_time,
    STDEV(machine_cycle_time) AS stdev_cycle_time,
    MIN(import_timestamp) AS first_import,
    MAX(import_timestamp) AS last_import,
    COUNT(DISTINCT [date]) AS active_days,
    COUNT(DISTINCT materialnumber) AS unique_materials,
    COUNT(DISTINCT operationnumber) AS unique_operations
FROM staging.forcam.cycle_time
WHERE te_sap IS NOT NULL
GROUP BY machine_name;
GO

-- =====================
-- Stored Procedures
-- =====================

-- Cleanup old data procedure
CREATE OR ALTER PROCEDURE forcam.sp_cleanup_old_data
    @days_to_keep INT = 90,
    @batch_size INT = 10000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @cutoff_date DATETIME2(3) = DATEADD(DAY, -@days_to_keep, GETDATE());
    DECLARE @rows_deleted INT;
    
    PRINT 'Starting cleanup of data older than: ' + CONVERT(NVARCHAR(30), @cutoff_date, 121);
    
    -- Clean cycle_time data
    WHILE 1 = 1
    BEGIN
        DELETE TOP (@batch_size) FROM staging.forcam.cycle_time
        WHERE import_timestamp < @cutoff_date;
        
        SET @rows_deleted = @@ROWCOUNT;
        IF @rows_deleted = 0 BREAK;
        
        PRINT 'Deleted ' + CAST(@rows_deleted AS NVARCHAR(10)) + ' rows from cycle_time';
        WAITFOR DELAY '00:00:01'; -- Brief pause to reduce blocking
    END
    
    -- Clean API data
    WHILE 1 = 1
    BEGIN
        DELETE TOP (@batch_size) FROM staging.forcam.api_cycle_data
        WHERE import_timestamp < @cutoff_date;
        
        SET @rows_deleted = @@ROWCOUNT;
        IF @rows_deleted = 0 BREAK;
        
        PRINT 'Deleted ' + CAST(@rows_deleted AS NVARCHAR(10)) + ' rows from api_cycle_data';
        WAITFOR DELAY '00:00:01';
    END
    
    -- Clean error logs (keep longer - 1 year)
    DELETE FROM staging.forcam.error_log
    WHERE error_date < DATEADD(YEAR, -1, GETDATE());
    
    PRINT 'Cleanup completed at: ' + CONVERT(NVARCHAR(30), GETDATE(), 121);
END
GO

-- Data quality check procedure
CREATE OR ALTER PROCEDURE forcam.sp_data_quality_check
    @machine_name NVARCHAR(100) = NULL,
    @check_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @check_date IS NULL SET @check_date = CAST(GETDATE() AS DATE);
    
    SELECT 
        'Data Quality Report' AS report_type,
        @check_date AS check_date,
        @machine_name AS machine_filter;
    
    -- Missing data check
    SELECT 
        'Missing Data' AS check_type,
        machine_name,
        COUNT(*) AS issues,
        'Records with NULL te_sap values' AS description
    FROM staging.forcam.cycle_time
    WHERE (@machine_name IS NULL OR machine_name = @machine_name)
      AND [date] = @check_date
      AND te_sap IS NULL
    GROUP BY machine_name
    HAVING COUNT(*) > 0;
    
    -- Outlier detection (cycle times > 3 standard deviations)
    WITH stats AS (
        SELECT 
            machine_name,
            AVG(machine_cycle_time) AS avg_cycle_time,
            STDEV(machine_cycle_time) AS stdev_cycle_time
        FROM staging.forcam.cycle_time
        WHERE [date] >= DATEADD(DAY, -7, @check_date)
          AND te_sap IS NOT NULL
          AND (@machine_name IS NULL OR machine_name = @machine_name)
        GROUP BY machine_name
    )
    SELECT 
        'Outliers' AS check_type,
        ct.machine_name,
        COUNT(*) AS issues,
        'Cycle times beyond 3 standard deviations' AS description
    FROM staging.forcam.cycle_time ct
    INNER JOIN stats s ON ct.machine_name = s.machine_name
    WHERE ct.[date] = @check_date
      AND ct.te_sap IS NOT NULL
      AND ABS(ct.machine_cycle_time - s.avg_cycle_time) > 3 * s.stdev_cycle_time
    GROUP BY ct.machine_name;
END
GO

PRINT 'FORCAM database schema creation completed successfully';
PRINT 'Tables created: api_cycle_data, cycle_time, error_log, import_stats';
PRINT 'Views created: Multiple summary views for different time periods';
PRINT 'Procedures created: sp_cleanup_old_data, sp_data_quality_check';
GO
