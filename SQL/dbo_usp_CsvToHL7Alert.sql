SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_CsvToHL7Alert]

AS 

BEGIN

 

                BEGIN TRY

 

                                                DECLARE  @LastRunID  INT

                                                ,                               @tableHTML NVARCHAR(MAX)

                                                ,                               @Count                              INT

                                                ,                               @AuditId BIGINT;

 

                                                DECLARE @CurrentDate Datetime            

                                                DECLARE @ERROR VARCHAR(1100) ,@ProcName VARCHAR(500), @StatusID  TINYINT;

 

                                               

 

                                                SELECT                  @LastRunID = CAST(ISNULL([ParamValue],0) AS INT)

                                                FROM                   [dbo].[tbl_Parameters] (Nolock)

                                                WHERE                 [ParamName]='Last Send Alert ID'

 

                                                Print CAST (@LastRunID AS VARCHAR)

                                                SELECT @ProcName=OBJECT_NAME(@@PROCID)

                                                SET @CurrentDate =GETDATE();

                                                EXEC [dbo].[usp_AuditLogs] 1,@ProcName,'Process Started',@AuditId OUT

 

                                                IF OBJECT_ID('tempdb..[#TEMP_COUNT]') IS NOT NULL DROP TABLE [#TEMP_COUNT] 

 

                                                CREATE TABLE [#TEMP_COUNT]

                                                (

                                                                [ID]                                                        INT ,

                                                                [CsvFileName]                   VARCHAR(3000),

                                                                [Status]                                VARCHAR(500),

                                                                [IMPORTDATE]                  DATE,

                                                                [ErrorDetail]                       VARCHAR(3000),

                                                )

 

                                                INSERT INTO [#TEMP_COUNT]

                                                (

                                                                                [ID]

                                                ,                               [CsvFileName]  

                                                ,                               [Status]               

                                                ,                               [IMPORTDATE] 

                                                ,                               [ErrorDetail]      

                                                )

                                                SELECT

                                                                                [ID]

                                                ,                               [CsvFileName]  

                                                ,                               [Status]               

                                                ,                               [DataReceiveDate]          

                                                ,                               [ErrorDetail]

                                                FROM   [dbo].[vw_ExternalSysDataImport]

                                                WHERE [ID]>@LastRunID

                                                ORDER BY  [ID] DESC

 

 

 

                                SELECT @Count =ISNULL(Count(1),0)

                                FROM [#TEMP_COUNT]

 

                                IF (@Count>0)

                                BEGIN

                                                                                SET @tableHTML = N'Hi,<br/><br/><br/><br/> <h1>CSV to Hl7 </h1> <br/><br/><br/><table border="1">' +

                                                                                N'<tr><th>Import Date</th>'+

                                                                                N'<th>Status</th>'+

                                                                                N'<th>File Name</th>'+

                                                                                N'<th>Error Detail</th>'+

 

                                                                                CAST ( (

                                                                                SELECT                                                                                                  td=[IMPORTDATE]               , '',

                                                                                                                                                                                                                td=[Status]                   , '',

                                                                                                                                                                                                                td=[CsvFileName]                                              , '',

                                                                                                                                                                                                                td=[ErrorDetail]                                   , ''

                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

                                                                                                                               

                                                                                FROM [#TEMP_COUNT]

                                                                                --Where CollectionID IS NOT NULL 

                                                                                --GROUP BY CONVERT(varchar(500), Hospital) 

                                                                                FOR XML PATH('tr'), TYPE) AS NVARCHAR(MAX) ) + '</table>

                                                                                <br/>

 

                                                                                You can place Another File to Process

                                                                                <br/><br/>';

 

                                                                                SELECT  @Count =ISNULL(MAX([ID]),@LastRunID) FROM [#TEMP_COUNT]

 

                                                                                UPDATE [dbo].[tbl_Parameters]

                                                                                SET                         [ParamValue]=CAST(@Count AS VARCHAR)

                                                                                WHERE [ParamName]='Last Send Alert ID'

 

                                END

 

                                IF (ISNULL(@tableHTML,'')<>'')

                                BEGIN

                                                                DECLARE @EmailRecipients NVARCHAR(MAX)

                                                                DECLARE @EmailSubject NVARCHAR(MAX)

                                                                DECLARE @EmailCC NVARCHAR(MAX)

                                                                  --SET Email details

                                                                SET @EmailRecipients = 'Louis.Carrillo@houstontx.gov'

                                                                SET @EmailCC = 'Kavitha.Sriram@houstontx.gov; ;

                                                                                                                                Yiqiao.Wang@houstontx.gov;

                                                                                                                                aun.ali@houstontx.gov'

                                                                SET @EmailSubject = 'CSV to HL7 Alert ' + CONVERT(nvarchar, GETDATE() ,101)

                                                                EXEC [dbo].[SendDBEmail] @tableHTML, @EmailRecipients, @EmailCC, @EmailSubject, 1 ,0

                                END

 

                                EXEC [dbo].[usp_AuditLogs] 0,@ProcName,'Process Ended',@AuditId     

                END TRY

                BEGIN CATCH

                END CATCH

 

               

END
GO
