SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_AuditLogs]

( 

                 @isStarted                                                         BIT                                                                          -- 1 for Start and 2 for End 

                ,@Procedure                                                      VARCHAR(50)                                    -- Process/Procedure name

                ,@Description                                    VARCHAR(3000)

                ,@AuditID                                   BIGINT                         OUTPUT

) 

AS 

                BEGIN 

                                BEGIN 

                                                DECLARE @CurrentDataTime DATETIME;

                                                SET @CurrentDataTime=GETDATE();

                                                IF @isStarted = 1 --Job Begning 

                                                                BEGIN 

 

                                                                                INSERT INTO [dbo].[vw_MedSync_AuditLogs] ([Description],[Procedure],[StartDateTime])

                                                                                SELECT @Description,@Procedure,@CurrentDataTime

                                               

                                                                                SET  @AuditID = Scope_Identity();

                                                                END 

                                                ELSE  

                                                                BEGIN 

                                                                                UPDATE [dbo].[vw_MedSync_AuditLogs] 

                                                                                SET                         [EndDateTime] = @CurrentDataTime 

                                                                                ,                               [Description] = @Description

                                                                                WHERE [Procedure]        = @Procedure 

                                                                                AND                       [AuditID] =@AuditID

                                                                END 

                                END 

                END
GO
