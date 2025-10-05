SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ConvertDataRowXML]

AS 

BEGIN

 

                                DECLARE @Count INT, @Counter INT ;

                                DECLARE @RawDataXML XML,                   @TSGXMLDATA XML,

                                @ExternalSystemID INT  , @ImportID INT,

                                @StatusID  TINYINT;

                                DECLARE @ERROR VARCHAR(1100) ,@ProcName VARCHAR(500);

                                SELECT @ProcName=OBJECT_NAME(@@PROCID)

                                DECLARE @AuditId BIGINT

                                DECLARE @TotalRowCount INT

                                EXEC [dbo].[usp_AuditLogs] 1,@ProcName,'Process Started',@AuditId OUT

                                DECLARE @CurrentDate Datetime;

                                DECLARE @UseColumnName Bit

                                DECLARE @FirstRowColumn Bit

                               

                                DECLARE @TempTable TABLE ([URN] INT IDENTITY(1,1), [ID] INT ,[ExternalSystemID] TINYINT,[DataReceiveDate] DATETIME,[RawDataXML] XML)

                                SET @StatusID=1 --IMPORTED

                                BEGIN TRY

                                SET @CurrentDate =GETDATE();

                                                -- =====================================================

                                                -- Check the We have XML With DATA Info

                                                -- ===========================================================

 

                                                INSERT INTO @TempTable ([ID],[ExternalSystemID],[DataReceiveDate],[RawDataXML])

                                                SELECT                  [ID],[ExternalSystemID],[DataReceiveDate],[RawDataXML]

                                                FROM                   [dbo].[vw_ExternalSysDataImport]

                                                WHERE                 [DataParseDateTime] IS NULL

                                                AND                                       [StatusID]=@StatusID -- IMPORTED

                                                AND                                       RawDataXML IS NOT NULL

 

                                                -- Test

                                                --Select * from @TempTable

 

                                                SET @Counter=0;

                                                SELECT @Count = Count(1) FROM @TempTable

 

                                                IF (@Count>@Counter)

                                                                BEGIN

                                                                                WHILE (@Count>@Counter)

                                                                                                BEGIN

                                                                               

                                                                                                                DECLARE @RootElement NVARCHAR(1000)

                                                                                                                DECLARE @strCommand NVARCHAR(MAX)

                                                                                                               

                                                                                                                SET @Counter = @Counter + 1;

                                                                                                                SELECT TOP 1 @RawDataXML = [RawDataXML]  ,@ExternalSystemID = [ExternalSystemID] ,@ImportID =[ID]

                                                                                                                FROM        @TempTable

                                                                                                                WHERE                 [URN]=@Counter;

 

                                                                                                                -- Test

                                                                                                                --Select * from @TempTable WHERE                        [URN]=@Counter;

                                                                                                                -- ========================================

                                                                                                                --             COUNT THE NODES

                                                                                                                -- ========================================

                                                                                                                DECLARE @TotalRowTable TABLE ([TotalCount] INT)

                                                                                                                -- CLEAR

                                                                                                                DELETE FROM @TotalRowTable;

 

 

                                                                                                                SELECT  @RootElement=[RootElement]

                                                                                                                FROM   [dbo].[vw_DIC_ExternalSys]

                                                                                                                WHERE [ExternalSystemID]=@ExternalSystemID

                                                                               

                                                                                                                SET  @strCommand= '

                                                                                                                                                                DECLARE @RawDataXML XML ,@RootElement NVARCHAR(1000);

                                                                                                                               

                                                                                                                                                                SET @RawDataXML='''+CAST (@RawDataXML AS VARCHAR(MAX)) + '''

                                                                                                                                                                SET @RootElement='''+CAST (@RootElement AS NVARCHAR(1000)) + '''

                                                                                                                               

                                                                                                                                                                SELECT @RawDataXML.value(''count(' + @RootElement +')'', ''INT'')

                                                                                                                '

                                                                                                                Print @strCommand

                                                                                                                INSERT INTO @TotalRowTable ([TotalCount])

                                                                                                                EXECUTE sp_executesql  @strCommand

 

                                                                                                                                SELECT TOP 1 @TotalRowCount=[TotalCount] FROM @TotalRowTable

                                                                               

                                                                                                                -- =====================================================

                                                                                                                -- Check the We have XML With ROW DATA

                                                                                                                -- ===========================================================

                                                                                                                -- Test

                                                                                                                --select  * from @TotalRowTable

 

                                                                                                                IF (@TotalRowCount>0)

                                                                                                                                BEGIN

                                                                                                                                  --------------------------------------------------

                                                                                                                                  --- SPLIT EACH ROW -----------------------------

                                                                                                                                  --------------------------------------------------

 

                                                                                                                                  IF OBJECT_ID('tempdb..[#ROWTABLE]') IS NOT NULL DROP TABLE [#ROWTABLE]

                                                                                                                                --- DECLARE @ROWTABLE  TABLE ([URN] INT IDENTITY(1,1), [ID] INT ,[ExternalSystemID] TINYINT,[RawDataXML] XML)

                                                                                                                                 

                                                                                                                                  

                                                                                                                                                CREATE TABLE [#ROWTABLE] ([URN] INT IDENTITY(1,1), [ID] INT ,[ExternalSystemID] TINYINT,[RawDataXML] XML)

                                                                                                                                               

 

                                                                                                                                --- CLEAN ROW

                                                                                                                                 -- DELETE FROM @ROWTABLE

                                                                                                                                  DELETE FROM [#ROWTABLE]

                                                                                                                               

                                                                                                                                  SET  @strCommand= '

                                                                                                                                                DECLARE @ImportID INT

                                                                                                                                                SET                         @ImportID='  + CAST(@ImportID AS VARCHAR) + '

                                                                                                                                 

                                                                                                                                                SELECT  

                                                                                                                                                  T.ID

                                                                                                                                                , T.ExternalSystemID

                                                                                                                                                , X.Y.query(''.'')

                                                                                                                                                FROM [dbo].[tbl_ExternalSysDataImport] T

                                                                                                                                                CROSS APPLY T.[RawDataXML].nodes(''' + @RootElement+ ''') X(Y)

                                                                                                                                                WHERE StatusID ='+ CAST( @StatusID AS varchar) +' and ID =@ImportID'

                                                                                                                                               

                                                                                                                                                Print @strCommand

                                                                                                                                               

                                                                                                                                                --INSERT INTO @ROWTABLE ([ID] ,[ExternalSystemID],[RawDataXML])

                                                                                                                                                INSERT INTO [#ROWTABLE] ([ID] ,[ExternalSystemID],[RawDataXML])

                                                                                                                                                EXECUTE sp_executesql  @strCommand

 

                                                                                                                                                Select * From [#ROWTABLE]

                                                                                                                                                -------------------------------------------------

                                                                                                                                --/// JUST TO USE THE COLUMN NAME INSTEAD OF NUMBER///

                                                                                                                                SELECT @UseColumnName =ISNULL([UseColumnName],0),

                                                                                                                                @FirstRowColumn=ISNULL([FirstRowHeader],0)

                                                                                                                                FROM [dbo].[vw_DIC_ExternalSys]

                                                                                                                                WHERE [ExternalSystemID]=@ExternalSystemID

                                                                                                                               

                                                                                                                                IF (@UseColumnName=1)

                                                                                                                                                BEGIN

 

 

                                                                                                                                                   IF(@FirstRowColumn<>1)

                                                                                                                                                       RAISERROR ('Error : Cannot use Column Name if First row in not Column Header', -- Message text. 

                                                                                                                                                                                   16, -- Severity. 

                                                                                                                                                                                   1 -- State. 

                                                                                                                                                                                   ); 

 

                                                                                                                                                                  IF OBJECT_ID('tempdb..[#NameChangeTable]') IS NOT NULL DROP TABLE [#NameChangeTable]

                                                                                                                                               

                                                                                                                                                               

                                                                                                                                                                                CREATE TABLE [#NameChangeTable]([RUN] INT IDENTITY (1,1),[OldColumnName] VARCHAR(1000) , [NewColumnName] VARCHAR(1000))

                                                                                                                                               

                                                                                                                                                                --DECLARE @NameChangeTable TABLE ([RUN] INT IDENTITY (1,1),[OldColumnName] VARCHAR(1000) , [NewColumnName] VARCHAR(1000))

                                                                                                                                                                DELETE FROM [#NameChangeTable]

 

                                                                                                                                                                INSERT INTO [#NameChangeTable] ([OldColumnName],[NewColumnName])

                                                                                                                                                                SELECT

                                                                                                                                                                                X.Y.value('fn:local-name(.[1])', 'varchar(1000)'),

                                                                                                                                                                                X.Y.value('.[1]', 'varchar(1000)')

                                                                                                                                                                --FROM @ROWTABLE T

                                                                                                                                                                FROM    [#ROWTABLE] T

                                                                                                                                                                CROSS APPLY T.[RawDataXML].nodes ('row//*') X(Y)

                                                                                                                                                                WHERE URN=1

                                                                                                                               

                                                                                                                                     

                                                                                                                                                  DECLARE @iChangeCount INT , @iChangeCounter  INT

                                                                                                                                                  DECLARE @OldColumnName VARCHAR(1000) , @NewColumnName VARCHAR(1000)

                                                                                                                                                 

                                                                                                                                                  SELECT @iChangeCount=COUNT(1) FROM [#NameChangeTable]

 

                                                                                                                                                  SET @iChangeCounter=0;

                                                                                                                                                  IF (@iChangeCount>@iChangeCounter)

                                                                                                                                                                BEGIN

                                                                                                                                                                                WHILE (@iChangeCount>@iChangeCounter)

                                                                                                                                                                                                BEGIN

                                                                                                                                                                                                   SET @iChangeCounter= @iChangeCounter+1

                                                                                                                                                                                                               

                                                                                                                                                                                                                SELECT [OldColumnName] ,[NewColumnName]

                                                                                                                                                                                                                FROM [#NameChangeTable]

                                                                                                                                                                                                                WHERE [RUN]=@iChangeCounter

 

                                                                                                                                                                                                                SELECT @OldColumnName =[OldColumnName] +'>' ,@NewColumnName=REPLACE([NewColumnName],'#','')+'>'

                                                                                                                                                                                                                FROM [#NameChangeTable]

                                                                                                                                                                                                                WHERE [RUN]=@iChangeCounter

 

                                                                                                                                                                                                                UPDATE T1

                                                                                                                                                                                                                SET [RawDataXML]  =replace(cast([RawDataXML] as nvarchar(max)),@OldColumnName,@NewColumnName)

                                                                                                                                                                                                               --FROM @ROWTABLE T1

                                                                                                                                                                                                                FROM    [#ROWTABLE] T1

                                                                                                                                                                                                                WHERE URN<>1

                                                                                                                                                                                                END

                                                                                                                                                                END

                                                                                                                                                               

                                                                                                                                     

                                                                                                                                                               

                                                                                                                                                END

                                                                                                               

 

                                                                                                                SELECT  [ID],                                       [ExternalSystemID] ,1, @FirstRowColumn,URN,

                                                                                                                                                                @CurrentDate,  [RawDataXML],

                                                                                                                                                                @CurrentDate,  @CurrentDate

                                                                                                                --             FROM    @ROWTABLE

                                                                                                                                FROM    [#ROWTABLE]

                                                                                                                                WHERE  ( URN<>1 or @FirstRowColumn<>1)

                                                                                                                               

                                                                                                                                INSERT INTO [vw_ExternalSysDataImportRow]

                                                                                                                                (

                                                                                                                                                                [ExternalSysData],                           [ExternalSystemID],                        [StatusID],

                                                                                                                                                                [DataReceiveDate],                          [RawDataXML],

                                                                                                                                                                [MedSync_Created_Date],                     [MedSync_Updated_Date]

                                                                                                                                )

                                                                                                                                SELECT  [ID],                                       [ExternalSystemID] ,1,

                                                                                                                                                                @CurrentDate,  [RawDataXML],

                                                                                                                                                                @CurrentDate,  @CurrentDate

                                                                                                                                --FROM    @ROWTABLE

                                                                                                                                FROM    [#ROWTABLE]

                                                                                                                                WHERE  ( URN<>1 OR @FirstRowColumn<>1)

                                                                                                                               

                                                                                                                                UPDATE [dbo].[vw_ExternalSysDataImport]

                                                                                                                                SET [DataParseDateTime] =@CurrentDate

                                                                                                                                WHERE [ID] =@ImportID

                                                                                                                               

                                                                                                                                END

                                                                                                                               

                                                                                                                               

 

                                               

 

                                                                                                END  -- WHILE

                                                                               

                                                                END --IF

 

                                                                SET @ERROR='Total Number of Record effected:' + CAST( @Count as VARCHAR)

                                                                EXEC [dbo].[usp_AuditLogs] 0,@ProcName,@ERROR,@AuditId

                                                END TRY

                                                BEGIN CATCH

                                                                               

                                                                                SET @ERROR=CAST(ERROR_NUMBER() AS VARCHAR(10)) + ' - ' +  CAST (ERROR_MESSAGE() AS VARCHAR(1000))

                                                                               

                                                                                UPDATE [dbo].[vw_ExternalSysDataImport]

                                                                                SET                         [ErrorDetail]=@ERROR

                                                                                                                ,[StatusID]=@StatusID+1-- Error

                                                                                WHERE   [ID]=@ImportID

                                                                                               

                                                                    EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ERROR ,@StatusID,@AuditId

                                                               

                                                END CATCH

END
GO
