SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ExternalDataImportTable]

( 

                 @StatusID                                                          INT                                         -- Process/Procedure name

                ,@MsgID                                                                             INT

                ,@ErrorMessage           VARCHAR(max)

) 

AS 

                BEGIN 

                               

                                                DECLARE @CurrentDataTime DATETIME;

                                               

                                                DECLARE @ProcName VARCHAR(500) ,@ErrorLog VARCHAR(MAX)            , @AuditId BIGINT ;

                                                SET @CurrentDataTime=GETDATE();

                                                SELECT @ProcName=OBJECT_NAME(@@PROCID)

 

                                    EXEC [dbo].[usp_AuditLogs] 1,@ProcName,'Process Started',@AuditId OUT

 

                                                UPDATE [dbo].[tbl_ExternalSysDataImport]

                                                SET  [StatusID]=@StatusID

                                                WHERE [ID] =@MsgID

                                               

                                                IF (@StatusID=10)

                                                BEGIN

                                                  SET @ErrorMessage=ISNULL(@ErrorMessage,'');

                                                                EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ErrorMessage,@StatusID,0

                                                END

                                  

                                   EXEC [dbo].[usp_AuditLogs] 0,@ProcName,'Total Number of Record effected: 1' ,@AuditId

               

                END
GO
