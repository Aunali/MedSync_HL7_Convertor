SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ValidateData]

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

                               

                                DECLARE @TempTable TABLE ([URN] INT IDENTITY(1,1), [ID] INT ,[ExternalImportID] INT,[ExternalSystemID] TINYINT,[DataReceiveDate] DATETIME,[RawDataXML] XML, [Validate] CHAR(1))

                               

                               

                                SET @StatusID=1 --IMPORTED

                                BEGIN TRY

                                SET @CurrentDate =GETDATE();

                                                -- =====================================================

                                                -- Check the We have XML With DATA Info

                                                -- ===========================================================

 

                                                INSERT INTO @TempTable ([ID],[ExternalImportID],[ExternalSystemID],[DataReceiveDate],[RawDataXML],[Validate])

                                                SELECT                  C.[ID],P.ID,C.[ExternalSystemID],C.[DataReceiveDate],C.[RawDataXML],'Y'

                                                FROM                   [dbo].[vw_ExternalSysDataImport]  P

                                                INNER JOIN         [dbo].[tbl_ExternalSysDataImportRow] C

                                                ON                                         P.ID=C.ExternalSysData

                                                WHERE                 P.[DataParseDateTime] IS NOT NULL

                                                AND                                       C.[DataParseDateTime] IS NULL

                                                AND                                       P.[StatusID]=@StatusID -- IMPORTED

                                                AND                                       C.[StatusID]=@StatusID -- IMPORTED

                                                AND                                       C.RawDataXML IS NOT NULL

 

                                                -- Test

                                                --Select * from @TempTable

 

                                                SET @Counter=0;

                                                SELECT @Count = Count(1) FROM @TempTable

 

                                                IF (@Count>@Counter)

                                                                BEGIN

                                               

                                                                                                                DECLARE @RootElement NVARCHAR(1000)

                                                                                                                DECLARE @strCommand NVARCHAR(MAX)

                                                                                                               

                                                                                                                SET @Counter = @Counter + 1;

                                                                                                                --SELECT TOP 1 @RawDataXML = [RawDataXML]  ,@ExternalSystemID = [ExternalSystemID] ,@ImportID =[ExternalImportID]

                                                                                                                --FROM      @TempTable

                                                                                                                --WHERE                              [URN]=@Counter;

 

                                                                                                                -- Test

                                                                                                                --Select * from @TempTable WHERE                        [URN]=@Counter;

                                                                                                                -- ========================================

                                                                                                                --             COUNT THE NODES

                                                                                                                -- ========================================

                                                                                               

                                                                                                                -- =====================================================

                                                                                                                -- Check the We have XML With ROW DATA

                                                                                                                -- ===========================================================

                                                                                                                -- Test

                                                                                                                --select  * from @TotalRowTable

 

 

                                                                                                                ------- APPLY RULE ----------------------

 

 

                                                                                                                -----------------------------------------

 

 

 

                                                                                                                UPDATE                T1

                                                                                                                SET                                         T1.[StatusID] = 3 --- Validate,

                                                                                                                                                ,               T1.[DataParseDateTime] = @CurrentDate

                                                                                                                FROM                   [dbo].[tbl_ExternalSysDataImportRow] T1

                                                                                                                INNER JOIN  @TempTable T2

                                                                                                                ON                                         T1.[ID]=T2.[ID]

                                                                                                                WHERE                 T2.[Validate] ='Y'

 

                                                                                                               

                                                                                                                UPDATE                T1

                                                                                                                SET                                         T1.[StatusID] = 3 --- Validate,

                                                                                                                FROM                   [dbo].[tbl_ExternalSysDataImport] T1

                                                                                                                WHERE                 T1.[StatusID] =@StatusID

                                                                                                                AND                                       EXISTS

                                                                                                                                                                (

                                                                                                                                                                                SELECT TOP 1 1 FROM   @TempTable T2

                                                                                                                                                                                WHERE T2.[ExternalImportID] = T1.[ID]

                                                                                                                                                                )

                                                               

                                                                               

                                                                END --IF

 

                                                                SET @ERROR='Total Number of Record effected:' + CAST( @Count as VARCHAR)

                                                                EXEC [dbo].[usp_AuditLogs] 0,@ProcName,@ERROR,@AuditId

                                                END TRY

                                                BEGIN CATCH

                                                                               

                                                                                SET @ERROR=CAST(ERROR_NUMBER() AS VARCHAR(10)) + ' - ' +  CAST (ERROR_MESSAGE() AS VARCHAR(1000))

                                                                               

                                                                                --UPDATE            [dbo].[vw_ExternalSysDataImport]

                                                                                --SET                      [ErrorDetail]=@ERROR

                                                                                --                             ,[StatusID]=4-- Error

                                                                                --WHERE   [ID]=@ImportID

                                                                                               

                                                                    EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ERROR ,@StatusID,@AuditId

                                                               

                                                END CATCH

END
GO
