SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ProcessCSV2XML]

AS 

                BEGIN 

                               

                                DECLARE @CurrentDataTime DATETIME;

                                                DECLARE @Procedure NVARCHAR(1000);

                                                SELECT @Procedure=OBJECT_NAME(@@PROCID)

                                                DECLARE @AuditId BIGINT

                                                EXEC [dbo].[usp_AuditLogs] 1,@Procedure,'Process Started',@AuditId OUT

                                               

                                                -- STEP 1 Convert INTO ROW INDIVIDUAL XML

                                                EXEC [dbo].[usp_ConvertDataRowXML]

                                               

                                                -- STEP 2  Convert INTO ONE STANDARD XML

                                                EXEC [dbo].[usp_ValidateData]

 

                                                -- STEP 3  Convert INTO ONE STANDARD XML

                                                EXEC [dbo].[usp_ConvertDataMedSyncXML]

                                                -- STEP 4  SEND ALERT TO USER 

                                                EXEC [dbo].[usp_CsvToHL7Alert]

                               

                                    EXEC [dbo].[usp_AuditLogs] 0,@Procedure      ,'Process ended',@AuditId

                                               

                END
GO
