SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ImportCsv_MirthConnect]

(
--@ExternalSystemID SMALLINT,
@FileName NVARCHAR(1000),
@RawXML XML,
@RawData NVARCHAR(MAX)
)

AS 

BEGIN

    DECLARE           @Count INT
                     ,@ExternalSystemID SMALLINT
                     ,@CurrentDate DateTime
                     ,@StatusID TINYINT
                     ,@ErrorMessage VARCHAR(MAX)
                     ,@RootElement VARCHAR(1000)
                     ,@strCommand NVARCHAR(MAX)
                     , @AuditId BIGINT;
    DECLARE @ProcName VARCHAR(500) ,@ErrorLog VARCHAR(MAX) ;


					-- DEBUG
                     INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@FileName', CAST(@FileName AS NVARCHAR(MAX)))
                     INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@RawXML', CAST(@RawXML AS NVARCHAR(MAX)))
                     INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@RawData', CAST(@RawData AS NVARCHAR(MAX)))

  
                BEGIN TRY

                                SELECT @ProcName=OBJECT_NAME(@@PROCID)
								EXEC [dbo].[usp_AuditLogs] 1,@ProcName,'Process Started',@AuditId OUT

                                IF OBJECT_ID('tempdb..[#COUNT]') IS NOT NULL DROP TABLE [#COUNT]
                                CREATE TABLE [#COUNT]([VALUE] INT)

                                 -- ============= INIT ===========================--
                                SET @CurrentDate=GETDATE();
                                SET @StatusID =1

								SELECT  TOP 1 @ExternalSystemID=[ExternalSystemID]
                                FROM  [dbo].[vw_DIC_ExternalSys]
                                WHERE   CHARINDEX([FileNameContain],@FileName)>0
                               
							   -- DEBUG
                                INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@ExternalSystemID', CAST(@ExternalSystemID AS NVARCHAR(MAX)))

                       

                                IF (@ExternalSystemID>0)

                                                BEGIN
											                 SELECT        @RootElement= [RootElement]
                                                             FROM          [dbo].[vw_DIC_ExternalSys]
                                                             WHERE         [ExternalSystemID]=@ExternalSystemID;

                                -- ==========================================================

                                                                IF @RawXML IS NULL
																	        BEGIN
                                                                                  SET  @ErrorLog=isNULL(@FileName,'') + ': Don`t have any Data ' + 'External SystemID: '+ CAST (@ExternalSystemID as VARCHAR(10))
                                                                                   EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ErrorLog,@StatusID
                                                                            END
                                                                SET @strCommand ='	   DECLARE  @XML XML
                                                                                        SET @XML = ''' +CAST (@RawXML AS NVARCHAR(MAX))+''' ;
                                                                                        INSERT INTO [#COUNT] ([VALUE])
                                                                                        SELECT @XML.value (''count(/'+@RootElement +')'', ''INT'');'

 
                                                                IF ISNULL(@strCommand,'')<>''

                                                                                BEGIN
																							PRINT @strCommand
                                                                                            EXECUTE sp_executesql  @strCommand

                                                                                END

                                                                Select @Count=[VALUE] FROM [#COUNT]
																-- DEBUG
                                                                INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@Count', CAST(@Count AS NVARCHAR(MAX)))

                                                                SET @RawData=REPLACE(@RawData,'''','');
																-- DEBUG
                                                                INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@@RawData', CAST('I AM HERE ' AS NVARCHAR(MAX)))

                               

                                                                SET @RawXML=CAST (REPLACE(CAST(@RawXML AS NVARCHAR(MAX)),'''','') AS XML) ;
																-- DEBUG
																INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@@RawXML', CAST(@RawXML AS NVARCHAR(MAX)))



																IF( @Count >0)
																				BEGIN
																							    INSERT INTO [dbo].[vw_ExternalSysDataImport] 
																										(	[ExternalSystemID],		[StatusID],			[DataReceiveDate],
																											[RawDataXML],			[RawData],			[CsvFileName],
																											[MedSync_Created_Date],	[MedSync_Updated_Date]
																										)
                                                                                                SELECT      @ExternalSystemID,		@StatusID,			@CurrentDate,
																											@RawXML,				@RawData,			@FileName,
																											@CurrentDate,			@CurrentDate
                                                                                END
                                                                ELSE
                                                                                BEGIN
                                                                                            SET  @ErrorLog=isNULL(@FileName,'') + ': Don`t have any Data ' + 'External SystemID: '+ CAST (@ExternalSystemID as VARCHAR(10))
                                                                                            EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ErrorLog,@StatusID,0
                                                                                END
                                                                SET @ErrorLog='Total Number of Record effected: 1'
                                                                INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD],[FIELDVALUE]) VALUES('@ErrorLog', CAST(@ErrorLog AS NVARCHAR(MAX)))
                                                                EXEC [dbo].[usp_AuditLogs] 0,@ProcName,@ErrorLog,@AuditId
                                                END
                                ELSE
                                                BEGIN
												                SET @ErrorLog='Total Number of Record effected: 0'
																EXEC [dbo].[usp_AuditLogs] 0,@ProcName,@ErrorLog,@AuditId
                                                                EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,'COULD NOT FIND EXTERNAL SYSTEM ID',@StatusID,@AuditId
                                                END
                END TRY
                BEGIN CATCH
                                SET  @ErrorLog=ERROR_MESSAGE();
                                EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ErrorLog,@StatusID,@AuditId
                END CATCH

    

END
GO
