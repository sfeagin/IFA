-- =====================
-- FORCAM CSV/JSON Import Script - Production Version
-- Version: 2.0
-- Description: Enhanced import script with proper error handling, validation, and logging
-- Parameters: MachineName, FilePath, FileType, BatchId
-- =====================

DECLARE @MachineName NVARCHAR(100) = '$(MachineName)';
DECLARE @FilePath NVARCHAR(500) = '$(FilePath)';
DECLARE @FileType NVARCHAR(10) = COALESCE('$(FileType)', '.csv');
DECLARE @BatchId UNIQUEIDENTIFIER = COALESCE(TRY_CAST('$(BatchId)' AS UNIQUEIDENTIFIER), NEWID());
DECLARE @ScriptVersion NVARCHAR(10) = '2.0';
DECLARE @StartTime DATETIME2(3) = GETDATE();
DECLARE @RecordsProcessed INT = 0;
DECLARE @RecordsInserted INT = 0;
DECLARE @RecordsSkipped INT = 0;

-- Validation
IF @MachineName IS NULL OR @MachineName = '' OR @MachineName = '$' + '(MachineName)'
BEGIN
    RAISERROR('MachineName parameter is required', 16, 1);
    RETURN;
END

IF @FilePath IS NULL OR @FilePath = '' OR @FilePath = '$' + '(FilePath)'
BEGIN
    RAISERROR('FilePath parameter is required', 16, 1);
    RETURN;
END

PRINT 'Starting import - Machine: ' + @MachineName + ', File: ' + @FilePath + ', Type: ' + @FileType + ', Batch: ' + CAST(@BatchId AS NVARCHAR(36));

BEGIN TRY
    BEGIN TRANSACTION ImportTransaction;

    -- Ensure schemas exist
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'staging')
        EXEC('CREATE SCHEMA staging');
    
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'forcam')
        EXEC('CREATE SCHEMA forcam');

    -- Ensure tables exist (creation handled by PowerShell script, but check here too)
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
            machine_cycle_time AS CAST(te_sap AS DECIMAL(18,6)),
            import_timestamp DATETIME2(3) NOT NULL DEFAULT GETDATE(),
            source_file NVARCHAR(500),
            batch_id UNIQUEIDENTIFIER DEFAULT NEWID(),
            INDEX IX_CycleTime_MachineName_Date (machine_name, [date]),
            INDEX IX_CycleTime_MaterialNumber (materialnumber),
            INDEX IX_CycleTime_ImportTimestamp (import_timestamp)
        );
        PRINT 'Created staging.forcam.cycle_time table';
    END

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
            INDEX IX_ErrorLog_ErrorDate (error_date),
            INDEX IX_ErrorLog_MachineName (machine_name)
        );
        PRINT 'Created staging.forcam.error_log table';
    END

    -- Process based on file type
    IF @FileType = '.csv'
    BEGIN
        PRINT 'Processing CSV file...';
        
        -- Create temporary table for raw CSV data
        CREATE TABLE #raw_csv (
            [date] DATE,
            [time] TIME(3),
            workplace NVARCHAR(50),
            ordernumber NVARCHAR(50),
            operationnumber NVARCHAR(50),
            materialnumber NVARCHAR(50),
            te_sap REAL,
            row_number INT IDENTITY(1,1)
        );

        -- Import CSV data with error handling
        BEGIN TRY
            BULK INSERT #raw_csv
            FROM @FilePath
            WITH (
                FIRSTROW = 2,              -- Skip header row
                FIELDTERMINATOR = ',',      -- CSV comma delimiter
                ROWTERMINATOR = '0x0A',     -- Line feed
                CODEPAGE = '65001',         -- UTF-8
                ERRORFILE = @FilePath + '.errors',
                MAXERRORS = 50,             -- Allow some bad rows
                TABLOCK                     -- Table lock for performance
            );
            
            SELECT @RecordsProcessed = COUNT(*) FROM #raw_csv;
            PRINT 'CSV bulk insert completed. Records loaded: ' + CAST(@RecordsProcessed AS NVARCHAR(10));
            
        END TRY
        BEGIN CATCH
            PRINT 'BULK INSERT failed: ' + ERROR_MESSAGE();
            -- Try alternative approach with OPENROWSET if BULK INSERT fails
            INSERT INTO #raw_csv ([date], [time], workplace, ordernumber, operationnumber, materialnumber, te_sap)
            SELECT 
                TRY_CAST(Col1 AS DATE),
                TRY_CAST(Col2 AS TIME(3)),
                Col3,
                Col4,
                Col5,
                Col6,
                TRY_CAST(Col7 AS REAL)
            FROM (
                SELECT 
                    value,
                    ROW_NUMBER() OVER (PARTITION BY RowNum ORDER BY ColNum) AS ColNum,
                    RowNum
                FROM (
                    SELECT 
                        value,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
                    FROM STRING_SPLIT((SELECT BulkColumn FROM OPENROWSET(BULK @FilePath, SINGLE_CLOB) AS x), CHAR(10))
                    WHERE LEN(value) > 0
                ) AS Rows
                CROSS APPLY (
                    SELECT 
                        value,
                        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ColNum
                    FROM STRING_SPLIT(Rows.value, ',')
                ) AS Cols
                WHERE RowNum > 1  -- Skip header
            ) AS Pivot
            PIVOT (
                MAX(value) FOR ColNum IN ([1] AS Col1, [2] AS Col2, [3] AS Col3, [4] AS Col4, [5] AS Col5, [6] AS Col6, [7] AS Col7)
            ) AS PivotTable;
            
            SELECT @RecordsProcessed = COUNT(*) FROM #raw_csv;
            PRINT 'Alternative CSV parsing completed. Records loaded: ' + CAST(@RecordsProcessed AS NVARCHAR(10));
        END CATCH

        -- Data validation and cleaning
        UPDATE #raw_csv 
        SET 
            workplace = LTRIM(RTRIM(workplace)),
            ordernumber = LTRIM(RTRIM(ordernumber)),
            operationnumber = LTRIM(RTRIM(operationnumber)),
            materialnumber = LTRIM(RTRIM(materialnumber))
        WHERE 
            workplace IS NOT NULL OR 
            ordernumber IS NOT NULL OR 
            operationnumber IS NOT NULL OR 
            materialnumber IS NOT NULL;

        -- Remove obviously invalid records
        DELETE FROM #raw_csv 
        WHERE [date] IS NULL 
           OR [time] IS NULL 
           OR workplace IS NULL 
           OR workplace = ''
           OR te_sap IS NULL
           OR te_sap < 0;

        SELECT @RecordsSkipped = @RecordsProcessed - @@ROWCOUNT;
        SET @RecordsProcessed = @@ROWCOUNT;
        
        PRINT 'Data validation completed. Valid records: ' + CAST(@RecordsProcessed AS NVARCHAR(10)) + ', Skipped: ' + CAST(@RecordsSkipped AS NVARCHAR(10));

        -- Insert into final table with MERGE for upsert capability
        MERGE staging.forcam.cycle_time AS target
        USING (
            SELECT
                @MachineName AS machine_name,
                [date],
                [time],
                workplace,
                ordernumber,
                operationnumber,
                materialnumber,
                te_sap,
                @FilePath AS source_file,
                @BatchId AS batch_id
            FROM #raw_csv
        ) AS source
        ON (
            target.machine_name = source.machine_name
            AND target.[date] = source.[date]
            AND target.[time] = source.[time]
            AND target.materialnumber = source.materialnumber
            AND target.operationnumber = source.operationnumber
        )
        WHEN NOT MATCHED THEN
            INSERT (machine_name, [date], [time], workplace, ordernumber, operationnumber, materialnumber, te_sap, source_file, batch_id)
            VALUES (source.machine_name, source.[date], source.[time], source.workplace, source.ordernumber, source.operationnumber, source.materialnumber, source.te_sap, source.source_file, source.batch_id)
        WHEN MATCHED THEN
            UPDATE SET
                te_sap = source.te_sap,
                import_timestamp = GETDATE(),
                source_file = source.source_file,
                batch_id = source.batch_id;

        SET @RecordsInserted = @@ROWCOUNT;
        DROP TABLE #raw_csv;
    END
    ELSE IF @FileType = '.json'
    BEGIN
        PRINT 'Processing JSON file...';
        
        -- Load JSON content
        DECLARE @json NVARCHAR(MAX);
        SELECT @json = BulkColumn FROM OPENROWSET(BULK @FilePath, SINGLE_CLOB) AS j;

        -- Validate JSON
        IF ISJSON(@json) = 0
        BEGIN
            RAISERROR('Invalid JSON format in file: %s', 16, 1, @FilePath);
            RETURN;
        END

        -- Parse and insert JSON data
        WITH ParsedJSON AS (
            SELECT *
            FROM OPENJSON(@json)
            WITH (
                [date] DATE '$.date',
                [time] TIME(3) '$.time',
                workplace NVARCHAR(50) '$.workplace',
                ordernumber NVARCHAR(50) '$.ordernumber',
                operationnumber NVARCHAR(50) '$.operationnumber',
                materialnumber NVARCHAR(50) '$.materialnumber',
                te_sap REAL '$.te_sap'
            )
            WHERE [date] IS NOT NULL 
              AND [time] IS NOT NULL 
              AND workplace IS NOT NULL 
              AND te_sap IS NOT NULL
              AND te_sap >= 0
        )
        MERGE staging.forcam.cycle_time AS target
        USING (
            SELECT
                @MachineName AS machine_name,
                [date],
                [time],
                workplace,
                ordernumber,
                operationnumber,
                materialnumber,
                te_sap,
                @FilePath AS source_file,
                @BatchId AS batch_id
            FROM ParsedJSON
        ) AS source
        ON (
            target.machine_name = source.machine_name
            AND target.[date] = source.[date]
            AND target.[time] = source.[time]
            AND target.materialnumber = source.materialnumber
            AND target.operationnumber = source.operationnumber
        )
        WHEN NOT MATCHED THEN
            INSERT (machine_name, [date], [time], workplace, ordernumber, operationnumber, materialnumber, te_sap, source_file, batch_id)
            VALUES (source.machine_name, source.[date], source.[time], source.workplace, source.ordernumber, source.operationnumber, source.materialnumber, source.te_sap, source.source_file, source.batch_id)
        WHEN MATCHED THEN
            UPDATE SET
                te_sap = source.te_sap,
                import_timestamp = GETDATE(),
                source_file = source.source_file,
                batch_id = source.batch_id;

        SET @RecordsInserted = @@ROWCOUNT;
        
        -- Count processed records from JSON
        SELECT @RecordsProcessed = COUNT(*)
        FROM OPENJSON(@json)
        WITH (
            [date] DATE '$.date',
            [time] TIME(3) '$.time',
            workplace NVARCHAR(50) '$.workplace',
            te_sap REAL '$.te_sap'
        );
        
        SET @RecordsSkipped = @RecordsProcessed - @RecordsInserted;
    END
    ELSE
    BEGIN
        RAISERROR('Unsupported file type: %s. Supported types: .csv, .json', 16, 1, @FileType);
        RETURN;
    END

    -- Log successful import
    DECLARE @Duration INT = DATEDIFF(MILLISECOND, @StartTime, GETDATE());
    DECLARE @SuccessMessage NVARCHAR(MAX) = 
        'Import completed successfully - ' +
        'Machine: ' + @MachineName + ', ' +
        'File: ' + @FilePath + ', ' +
        'Records Processed: ' + CAST(@RecordsProcessed AS NVARCHAR(10)) + ', ' +
        'Records Inserted/Updated: ' + CAST(@RecordsInserted AS NVARCHAR(10)) + ', ' +
        'Records Skipped: ' + CAST(@RecordsSkipped AS NVARCHAR(10)) + ', ' +
        'Duration: ' + CAST(@Duration AS NVARCHAR(10)) + 'ms, ' +
        'Batch ID: ' + CAST(@BatchId AS NVARCHAR(36));

    PRINT @SuccessMessage;

    COMMIT TRANSACTION ImportTransaction;
    
    -- Return success metrics
    SELECT 
        'SUCCESS' AS Status,
        @MachineName AS MachineName,
        @RecordsProcessed AS RecordsProcessed,
        @RecordsInserted AS RecordsInserted,
        @RecordsSkipped AS RecordsSkipped,
        @Duration AS DurationMs,
        @BatchId AS BatchId;

END TRY
BEGIN CATCH
    -- Rollback transaction on error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION ImportTransaction;

    -- Log the error
    DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorLine INT = ERROR_LINE();
    DECLARE @ErrorDetails NVARCHAR(MAX) = 
        'Import failed - ' +
        'Machine: ' + COALESCE(@MachineName, 'NULL') + ', ' +
        'File: ' + COALESCE(@FilePath, 'NULL') + ', ' +
        'Error: ' + @ErrorMessage + ', ' +
        'Line: ' + CAST(@ErrorLine AS NVARCHAR(10)) + ', ' +
        'Severity: ' + CAST(@ErrorSeverity AS NVARCHAR(10));

    PRINT 'ERROR: ' + @ErrorDetails;

    -- Insert error into log table
    BEGIN TRY
        INSERT INTO staging.forcam.error_log (
            error_message, error_date, error_severity, error_line,
            machine_name, file_path, script_version, batch_id
        )
        VALUES (
            @ErrorMessage, GETDATE(), @ErrorSeverity, @ErrorLine,
            @MachineName, @FilePath, @ScriptVersion, @BatchId
        );
    END TRY
    BEGIN CATCH
        PRINT 'Failed to log error: ' + ERROR_MESSAGE();
    END CATCH

    -- Re-throw the error
    THROW;
END CATCH;

PRINT 'Import script completed at: ' + CONVERT(NVARCHAR(30), GETDATE(), 121);
