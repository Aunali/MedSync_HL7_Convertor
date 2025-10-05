SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_Log_MedSync_ProcessError]

(

                @ErrorOccureAt VARCHAR(500)

                ,@ErrorDetail VARCHAR(MAX)

                ,@StatusID TINYINT

                ,@AuditID BIGINT

)

AS

                SET NOCOUNT ON;

               

BEGIN

               

                INSERT INTO [dbo].[vw_MedSync_ProcessError] ([ErrorOccureAt],[ErrorDetail],[StatusID],[Paged],[MedSync_Created_Date],[MedSync_Updated_Date])

                VALUES (@ErrorOccureAt,@ErrorDetail,@StatusID, 0,GETDATE(),GETDATE())

                IF @AuditID>0

                                EXEC [usp_AuditLogs] 0,@ErrorOccureAt,@ErrorDetail,@AuditID

END
GO
