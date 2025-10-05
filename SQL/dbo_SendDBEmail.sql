SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SendDBEmail]

                -- Add the parameters for the stored procedure here

                                @Email_Boby NVARCHAR(MAX)

                               

                                ,@Email_Recipients NVARCHAR(MAX)

                                ,@Email_CC NVARCHAR(MAX)

                                ,@Email_Subject  NVARCHAR(500)

                                ,@Email_IsbodyHtml BIT =0

                                ,@Email_IsImportance  BIT =0

AS

BEGIN

                               

                                BEGIN TRY

 

                                DECLARE @Email_@Body_Format VARCHAR(100)

                                ,                               @CurrentDate   DATETIME

                                ,                               @Email_Profile_Name VARCHAR(100)

                                ,                               @Email_Importance VARCHAR(100)

 

                                               

                                SET  @Email_@Body_Format ='TEXT'

                                SET  @Email_Importance ='Normal'

                               

                                SET @CurrentDate =GETDATE();

 

                                IF (ISNULL(@Email_Profile_Name,'')='')

                                BEGIN

                                                SET @Email_Profile_Name='HHDIT ALERT'

                                END

 

                                IF (@Email_IsbodyHtml=1)

                                BEGIN

                                                SET  @Email_@Body_Format ='HTML';

                                END

                                IF (@Email_IsImportance=1)

                                BEGIN

                                                SET  @Email_Importance ='High';

                                END

                               

 

                                EXEC [msdb].[dbo].[sp_send_dbmail]

                                @profile_name = @Email_Profile_Name,

                                @recipients=@Email_Recipients,

                                @copy_recipients = @Email_CC,

                                @subject = @Email_Subject,

                                @body = @Email_Boby,

                                @body_format =  @Email_@Body_Format,

                                @importance = @Email_Importance

 

                                                               

                                END TRY

                                BEGIN CATCH

                                               

                                                DECLARE @ERROR NVARCHAR(MAX)

                                                                                                                SET @ERROR = CAST(ERROR_NUMBER() AS VARCHAR(25)) +' - ' +CAST (ERROR_MESSAGE() AS varchar(MAX))

                                                                                                                SET @ERROR=@ERROR + ISNULL(@Email_Subject,'')

                                                                                                                --- IF NOT QULIFY AS HL7 MESSAGE

                                                                                                                               

                                                                                               

                                                                                Print @ERROR

 

 

 

                                END CATCH

 

END
GO
