SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_ConvertDataMedSyncXML]

AS 

BEGIN

               

               

                DECLARE @TEMTABLE TABLE  ([ID] INT IDENTITY (1,1) , [ExternalSystemID] INT,[HL7Segment] VARCHAR(10),[Position] TINYINT ,SeqNo TINYINT )

                DECLARE @CurrentDate Datetime

                DECLARE @ERROR VARCHAR(1100) ,@ProcName VARCHAR(500), @StatusID  TINYINT;

 

                DECLARE @StrCommand NVARCHAR(MAX),

                                                @SegName VARCHAR(10),

                                                @ExternalID INT,

                                                @MAPID INT,

                                                @Position TINYINT,

                                                @Counter INT ,

                                                @Count INT ,

                                                @SQL_SCRIPT  NVARCHAR(MAX),

                                                @AuditId BIGINT;

                               

                SELECT @ProcName=OBJECT_NAME(@@PROCID)

                SET @CurrentDate =GETDATE();

                IF OBJECT_ID('tempdb..[#TEMTABLE]') IS NOT NULL DROP TABLE [#TEMTABLE]

                --IF OBJECT_ID('tempdb..[#TEMTABLEMERG]') IS NOT NULL DROP TABLE [#TEMTABLEMERG]

 

                --CREATE TABLE [#TEMTABLEMERG] (       [ROWID] INT,[ExternalSysData] INT ,[ExternalSystemID] INT           )

 

                CREATE TABLE [#TEMTABLE]

                (

                [ROWID]                                                              INT,

                [ExternalSysData]            INT,

                [ExternalSystemID]         INT,

                [RAWXML]                                          XML,

                [HHDXML]                                           XML       ,

                [NotApplyTrans]                  BIT

                )

               

               

                BEGIN TRY

                EXEC [dbo].[usp_AuditLogs] 1,@ProcName,'Process Started',@AuditId OUT

                INSERT INTO [#TEMTABLE]  ([ROWID],[ExternalSysData],[ExternalSystemID],[RAWXML],[NotApplyTrans])

                SELECT                  T1.[ID],T1.[ExternalSysData],T1.[ExternalSystemID] ,T1.[RawDataXML],0

                FROM                   [dbo].[vw_ExternalSysDataImportRow] T1

                INNER JOIN  [dbo].[vw_ExternalSysDataImport] T2

                ON                                         (T1.[ExternalSysData] =T2.[ID])

                WHERE                 T1.[StatusID]=3

                AND                                       T2.[StatusID]=3

                AND                                       T2.[DataParseDateTime] IS NOT NULL

                AND                                       T1.[DataParseDateTime] IS NOT NULL

                AND                                       T1.[MedSyncDataXML] IS NULL

                AND                                       EXISTS  

                                                                                (

                                                                                                SELECT TOP         1 1

                                                                                                FROM                   [dbo].[vw_DIC_ExternalSys] T

                                                                                                WHERE                 T.Active=1

                                                                                                AND                                       T.ExternalSystemID=T2.ExternalSystemID

                                                                                )             

 

                SET @StatusID=5

 

 

                DELETE FROM                    @TEMTABLE

               

                INSERT INTO                       @TEMTABLE  ([ExternalSystemID] ,[HL7Segment],[Position],SeqNo            )

                SELECT                                  T.[ExternalSystemID] , T.[HL7Segment] ,ISNULL(T.[Position],1),T2.SeqNo  

                FROM                                   [vw_DIC_ExternalSysFieldMapping] T

                INNER JOIN                         [dbo].[tbl_DIC_ExternalSysSegmentOrder] T2

                ON                                                         T2.[ExternalSystemID] = T.[ExternalSystemID]

                AND                                                       T2.[Hl7Segment] = T.[Hl7Segment]

                WHERE                                 EXISTS (

                                                                                                                                                                                                SELECT TOP 1 1 FROM [#TEMTABLE] T1

                                                                                                                                                                                                WHERE T1.ExternalSystemID =   T.[ExternalSystemID])

                GROUP BY T.[ExternalSystemID],T.[HL7Segment],ISNULL(T.[Position],1),T2.SeqNo

                Order By T2.SeqNo         

               

                UPDATE @TEMTABLE SET [Position] = ISNULL([Position],1);

 

 

                SET @Counter=0;

                SELECT @Count = Count(1) FROM @TEMTABLE

 

                                IF (@Count>@Counter)

                                                                                                BEGIN

                                                                                                                WHILE (@Count>@Counter)

                                                                                                                                BEGIN

                                                                                                                                                SET @StrCommand='';

                                                                                                                                                SET @Counter = @Counter + 1;

                                                                                                                                                SELECT   @SegName=[HL7Segment] ,@ExternalID=[ExternalSystemID],@Position=[Position]  FROM @TEMTABLE WHERE [ID]=@Counter ;

                                                                                                                                               

                                                                                                                                --BEGIN TRY       

                                                                                                                                                IF ( OBJECT_ID('tempdb..[#TEMTABLE]')IS NOT NULL )

                                                                                                                                                BEGIN

                                                                                                                                                                 

                                                                                                                                                                 --IF  EXISTS (SELECT * FROM TempDB.INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME =  @SegName+CAST(@Position AS VARCHAR) +'XML' AND TABLE_NAME Like( '#TEMTABLE__%'))

                                                                                                                                                                --BEGIN

                                                                                                                                                                --INSERT INTO  [dbo].[tbl_debug] ([Key],[value],[HHD_Created_DATE]) VALUES('ADD COL EXISTS', @SegName+CAST(@Position AS VARCHAR) +'XML',GETDATE())

                                                                                                                                                                --END

                                                                                                                                                                --ELSE

                                                                                                                                                                --BEGIN

                                                                                                                                                                 -- INSERT INTO  [dbo].[tbl_debug] ([Key],[value],[HHD_Created_DATE]) VALUES('ADD COL NOT EXISTS', @SegName+CAST(@Position AS VARCHAR) +'XML',GETDATE())

                                                                                                                                                                --END

 

                                                                                                                                                               

                                                                                                                                                                                 SET @SQL_SCRIPT=' ALTER TABLE [#TEMTABLE] ADD [' +  @SegName+CAST(@Position AS VARCHAR) +'XML]  [XML]' 

                                                                                                                                                                                 print @SQL_SCRIPT

                                                                                                                                                                                EXECUTE (@SQL_SCRIPT)

                                                                                                                                                               

                                                                                                                                                END

                                                                                                                                                -- Select * FROM [#TEMTABLEMERG]

 

                                                                                                                                                DECLARE @strCommandTable TABLE (URN INT Identity(1,1) ,[strCommand] NVARCHAR(MAX))

                                                                                                                                                DELETE @strCommandTable

                                                                                                                                                --SELECT  @strCommand= @strCommand+

                                                                                                                                                INSERT INTO @strCommandTable ( [strCommand])

                                                                                                                                                SELECT

                                                                                                                                                   CASE

                                                                                                                                                                                WHEN ISNULL([XQueryPath],'')=''

                                                                                                                                                                                THEN

                                                                                                                                                                                                CASE

                                                                                                                                                                                                                                WHEN ISNULL([UDFFunction],'')<>''

                                                                                                                                                                                                                                -- THEN     ',  '+ [UDFFunction] + ' (x.m.value(''' +[XQueryPath] + ''',''varchar(MAX)''))  AS [' + LTRIM(RTRIM([FieldName]))  +']'

                                                                                                                                                                                                                                THEN     ',  '+ REPLACE ([UDFFunction] , '{#Input#}', ISNULL(''''+[DefaultValue]+'''',''))+'  AS [' + LTRIM(RTRIM([FieldName]))  +']'

                                                                                                                                                                                                                                ELSE                  ', ''' +  ISNULL([DefaultValue],'') +'''  AS [' + LTRIM(RTRIM([FieldName])) +']'

                                                                                                                                               

                                                                                                                                                                                                END

                                                                                                                                                                                ELSE

                                                                                                                                                                                                CASE

                                                                                                                                                                                                                WHEN ISNULL([UDFFunction],'')<>''

                                                                                                                                                                                                                -- THEN                 ',  '+ [UDFFunction] + ' (x.m.value(''' +[XQueryPath] + ''',''varchar(MAX)''))  AS [' + LTRIM(RTRIM([FieldName]))  +']'

                                                                                                                                                                                                                THEN    ',  '+ REPLACE ([UDFFunction] , '{#Input#}', ' (x.m.value(''' +ISNULL([XQueryPath],'') + ''',''varchar(MAX)''))')+'  AS [' + LTRIM(RTRIM([FieldName]))  +']'

                                                                                                                                                                                                                ELSE      ',  x.m.value(''' +ISNULL([XQueryPath],'') + ''',''varchar(MAX)'')  AS [' + LTRIM(RTRIM([FieldName]))  +']'

                                                                                                                                                                                                END

                                                                                                                                                               

                                                                                                                                                                               

                                                                                                                                                                               

                                                                                                                                                                END

                                                                                                                                               

                                                                                                                                               

                                                                                                                                                                                FROM                [dbo].[vw_DIC_ExternalSysFieldMapping]

                                                                                                                                                                                WHERE                [HL7Segment]=@SegName

                                                                                                                                                                                AND                       [ExternalSystemID]=@ExternalID

                                                                                                                                                                                AND                       ISNULL([Position],1)=@Position

                                                                                                                                                                                ORDER BY [SeqNo]

                                                                               

                                                                                                                                                                IF OBJECT_ID('tempdb..[#LOOPTABLE]') IS NOT NULL DROP TABLE #LOOPTABLE

                                                                                                                                                                CREATE TABLE #LOOPTABLE         ( [URN] INT IDENTITY (1,1) ,[ExternalSysData] INT )

 

 

                                                                                                                                                                --DECLARE @LOOPTABLE TABLE ( [URN] INT IDENTITY (1,1) ,[ExternalSysData] INT )

                                                                                                                                                                --DELETE FROM @LOOPTABLE

                                                                                                                                                                DELETE FROM #LOOPTABLE

                                                                                                                                                                DECLARE @CountLoopTab INT,@CounterLoopTab INT ,@ExternalSysData INT

 

                                                                                                                                                                --INSERT INTO @LOOPTABLE([ExternalSysData])

                                                                                                                                                                INSERT INTO #LOOPTABLE([ExternalSysData])

                                                                                                                                                                SELECT  DISTINCT [ExternalSysData] FROM [#TEMTABLE]

                                                                                                                                                               

                                                                                                                                                                --SELECT @CountLoopTab =COUNT(1) FROM @LOOPTABLE;

                                                                                                                                                                SELECT @CountLoopTab =COUNT(1) FROM #LOOPTABLE;

                                                                                                                                                               

                                                                                                                                                                SET @CounterLoopTab=0;

 

                                                                                                                                                                WHILE (@CountLoopTab>@CounterLoopTab)

                                                                                                                                                                BEGIN

 

                                                                                                                                                                                                SET @CounterLoopTab =@CounterLoopTab+1;

                                                                                                                                                                                               

                                                                                                                                                                                                --SELECT  @ExternalSysData =[ExternalSysData] FROM @LOOPTABLE WHERE URN =@CounterLoopTab;

                                                                                                                                                                                                SELECT  @ExternalSysData =[ExternalSysData] FROM #LOOPTABLE WHERE URN =@CounterLoopTab;

 

 

                                                                                                                                                                                                SELECT    @strCommand= @strCommand+ [strCommand] FROM @strCommandTable ORDER BY URN

                                                                                                                                                                                                SELECT @strCommand

 

                                                                                                                                                                                                                                                BEGIN TRY

 

                                                                                                                                                                                                                                                                SET @strCommand=' '''+ @SegName +''' [HL7SegName]  ' +@strCommand

                                                                                                                                                                                                                                                                                PRINT @strCommand

                                                                                                                                                                                                                                                                SET  @strCommand=N'

                                                                                                                                                                                                                               

                                                                                                                                                                                                                                                                                                                                UPDATE T

                                                                                                                                                                                                                                               

                                                                                                                                                                                                                                                                                                                                SET ['+ @SegName+ CAST(@Position AS VARCHAR) +'XML] =

                                                                                                                                                                                                                                                                                                                                CAST (( SELECT ' + @strCommand + '

                                                                                                                                                                                                                                                                                                                                FROM  [dbo].[#TEMTABLE] T1

                                                                                                                                                                                                                                                                                                                                CROSS APPLY [RAWXML].nodes(''/row'') x(m) 

                                                                                                                                                                                                                                                                                                                                WHERE T1.[ROWID]=T.[ROWID]

 

                                                                                                                                                                                                                                                                                                                                FOR XML PATH('''')

                                                                                                                                                                                                                                                                                                                                ) AS XML)

                                                                                                                                                                                                                                                                                                                                FROM [#TEMTABLE] T

                                                                                                                                                                                                                                                                                                                                WHERE [ExternalSysData] =' + CAST(@ExternalSysData AS VARCHAR(10) )  +'

 

 

                                                                                                                                                                                                                                                                '

                                                                                                                                                                                                                                                                Select @strCommand

                                                                                                                                               

                                                                                                                                                                                                                                                                EXECUTE sp_executesql  @strCommand

                                                                                                                                                                                                                                                                SET @strCommand=''

                                                                                                                               

                                                                                                                                               

                                                                                                                                                                                                                                                END TRY

                                                                                                                                                                                                                                                                                BEGIN CATCH

                                                                               

                                                                                                                                                                                                                                                                                                                SET @MAPID=0

                                                                                                                                                                                                                                                                                                                SET @ERROR= 'SegName' +ISNULL(@SegName,'')+'-' +CAST(@ExternalSysData AS VARCHAR)  + ' - '+ CAST(ERROR_NUMBER() AS VARCHAR(10)) + ' - ' +  CAST (ERROR_MESSAGE() AS VARCHAR(1000))

                                                                                                                                                                                                                                                                                                                print @strCommand

                                                                                                                                                                                                                                                                                                                UPDATE T1

                                                                                                                                                                                                                                                                                                                SET                         [ErrorDetail]=@ERROR

                                                                                                                                                                                                                                                                                                                                                ,[StatusID]=@StatusID+1-- Error

 

                                                                                                                                                                                                                                                                                                                                                FROM [dbo].[vw_ExternalSysDataImport] T1

                                                                                                               

                                                                                                                                                                                                                                                                                                                WHERE   [StatusID]=3

                                                                                                                                                                                                                                                                                                                AND       T1.ID=@ExternalSysData

                                                                                                                                                                                                                                                                                                                                                                                               

                                                                                                                                                                                                                                                                                                                UPDATE [#TEMTABLE]

                                                                                                                                                                                                                                                                                                                SET [NotApplyTrans]=1

                                                                                                                                                                                                                                                                                                                WHERE [ExternalSysData]=@ExternalSysData

 

                                                                                                                                                                                                                                                                                                                EXEC  [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ERROR ,@StatusID,@AuditId

                                                               

                                                                                                                                                                                                                END CATCH                                

                                                                                                                                                                                END -- INNER WHILE

                                                                                                                                END -- OUT WHILE

                                                               

                                                               

                                                                ---- ADD LOG

                                                                /*

                                                                Need to Change the for each file

                                                                */

                                                                                print 'ADD LOG'

                                                                                                BEGIN  /* Dynamic Query  */

 

                                                                               

                                                                                                                DECLARE  @iCounter int

                                                                                                                                                ,@Counts int

                                                                                                                                                ,@SQLQUERY varchar(max)

                                                                                                                                                ,@Desc varchar(200)

 

 

                                                                                                                IF OBJECT_ID('tempdb..[#RULE_TAB]') IS NOT NULL DROP TABLE [#RULE_TAB]                        

 

 

                                                                                                                CREATE TABLE [#RULE_TAB]

                                                                                                                (

                                                                                                                                ID  int identity (1,1),

                                                                                                                                SQLQRY varchar(max),

                                                                                                                                [Description] varchar(200)

 

                                                                                                                )

 

                                                                                                                                INSERT INTO [#RULE_TAB] (

                                                                                                                                                                                                                                SQLQRY,

                                                                                                                                                                                                                                [Description]

                                                                                                                                                                                                                                )

                                                                                                                                                SELECT                                                  [DynamicQuery],

                                                                                                                                                                                                                                [Description]

                                                                                                                                                FROM

                                                                                                                                                                                                                                [dbo].[vw_DIC_Transformation_Rules] T1

                                                                                                                                                WHERE                                                 [RuleType]='CSV TRANSFORMATION'

                                                                                                                                                AND                                                                       Active=1

                                                                                                                                                AND                                                       EXISTS

                                                                                                                                                                                                                (

                                                                                                                                                                                                                                SELECT  1

                                                                                                                                                                                                                                FROM   [#TEMTABLE] T2

                                                                                                                                                                                                                                WHERE T1.ExternalSystemID = T2.ExternalSystemID

                                                                                                                                                                                                               

                                                                                                                                                                                                                )

                                                                                                                                                ORDER BY                                            [SeqNo]

 

                                                                                                                                                SET @iCounter=1

                                                                                                                                                                Select @Counts=count(1) from  [#RULE_TAB]

 

                                                                                                                                                                IF (@Counts>0)

                                                                                                                                                                BEGIN

                                                                                                                                                                                WHILE(@Counts>=@iCounter)

                                                                                                                                                                                BEGIN

                                                                                               

                                                                                                                                                                                                SELECT  @SQLQUERY =SQLQRY ,@Desc=isnull([Description],'') FROM [#RULE_TAB] Where ID=@iCounter

                                                                                               

                                                                                                                                                                                                Print @SQLQUERY

                                                                                                                                                                                                Select @SQLQUERY

                                                                                                                                                                                                EXECUTE (@SQLQUERY)

                                                                                                                                                                                                SET @iCounter=@iCounter+1

                                                                                                                                                                                END

                                                                                                                                                                END

 

                                                                                                END

 

 

                                                                --- TO TRANSFORMATION LOGIC

                                                                ---- --------

                                                                print 'TRANSFORMATION LOGIC'

                                                                                                                --- MERGE XML  ---

                                                                                                                                SET @Counter=0;

                                                                                                                                SELECT @Count = Count(1) FROM @TEMTABLE

                                                                                                                                SET @strCommand=' '

                                                                                                                                WHILE (@Count>@Counter)

                                                                                                                                                BEGIN

                                                                                                                                                                 SET @Counter = @Counter + 1;

                                                                                                                                                                SELECT   @SegName=[HL7Segment], @Position=[Position] FROM @TEMTABLE WHERE [ID]=@Counter;

                                                                                                                                                               

                                                                                                                                                                 IF (@Counter=1)

                                                                                                                                                                                SET @strCommand = ' SELECT  ['+ @SegName + CAST(@Position AS VARCHAR)  +'XML]  AS ' + @SegName

                                                                                                                                                                 ELSE

                                                                                                                                                                                SET @strCommand = @strCommand +' ,  ['+ @SegName + CAST(@Position AS VARCHAR)  +'XML]  AS ' + @SegName

                                                                                                                                                                 

                                                                                                                                                END

                                                                                                                                                SET @strCommand = '

                                                                                                                                                UPDATE T

 

                                                                                                                                                SET [HHDXML] =  ('+ @strCommand +'

                                                                                                                                                                                                                FROM

                                                                                                                                                                                                                [#TEMTABLE] T1

                                                                                                                                                                                                                WHERE T1.[ROWID]=T.[ROWID]                                                                

                                                                                                                                                                                                                 FOR XML PATH ('''') , ROOT(''MavenHl7'') )

                                                                                                                                                FROM   [#TEMTABLE] T

                                                                                                                                                WHERE  ISNULL([NotApplyTrans],0) <>1

                                                                                                                                                                                                                                                                                               

                                                                                                                                                                                                                                                                                                '

                                                                                                                                SELECT @strCommand

                                                                                                                                EXECUTE sp_executesql  @strCommand

                                                                                                                                                                                SET @strCommand=''

                                                                                                               

                                                                                                                UPDATE T1

                                                                                                                SET                                         T1.[MedSyncDataXML]=T2.HHDXML

                                                                                                                                                                ,T1.[DataParseDateTime]=@CurrentDate

                                                                                                                                                                ,T1.[MedSync_Updated_Date]=@CurrentDate

                                                                                                                                                                ,T1.[StatusID]=5 -- Parsed

                                                                                                                FROM                   [dbo].[vw_ExternalSysDataImportRow] T1

                                                                                                                INNER JOIN         [#TEMTABLE] T2

                                                                                                                ON                                         T1.[ID]=T2.[ROWID];

                                                                                                               

                                                                                                               

                                                                                                                SELECT * FROM [#TEMTABLE]

 

                                                                                                                               

                                                                                                                UPDATE T1

                                                                                                                SET                                        

                                                                                                                                                                 T1.[DataParseDateTime]=@CurrentDate

                                                                                                                                                                ,T1.[MedSync_Updated_Date]=@CurrentDate

                                                                                                                                                                ,T1.[StatusID]=7 -- Ready for Converstion

                                                                                                                                                                ,T1.[MedSyncDataXML] =CAST((

                                                                                                                                                                                                                                                SELECT  [HHDXML]

                                                                                                                                                                                                                                                FROM   [#TEMTABLE]  T2

                                                                                                                                                                                                                                                WHERE T2.[ExternalSysData]=T1.[ID]

                                                                                                                                                                                                                                                FOR XML PATH (''),ROOT('BATCH')

                                                                                                                                                                                                                                               

                                                                                                                                                                                                                                                ) AS XML)

                                                                                                                FROM                   [dbo].[vw_ExternalSysDataImport] T1

                                                                                                                WHERE                 EXISTS

                                                                                                                                                                (

                                                                                                                                                                                SELECT  TOP 1 1

                                                                                                                                                                                FROM   [#TEMTABLE] T

                                                                                                                                                                                WHERE                T.[ExternalSysData]=T1.[ID]

                                                                                                                                                                                AND                       ISNULL([NotApplyTrans],0) <>1

                                                                                                                                                                )

 

 

                                                                                                END

                                                                EXEC [dbo].[usp_AuditLogs] 0,@ProcName,'Process Ended',@AuditId                     

 

                                                                IF OBJECT_ID('tempdb..[#TEMTABLE]') IS NOT NULL DROP TABLE [#TEMTABLE]

 

                                END TRY

                                                BEGIN CATCH

                                                                               

                                                                                SET @MAPID=0

                                                                                SET @ERROR= 'SegName' +ISNULL(@SegName,'')+'-' +CAST(@MAPID AS VARCHAR)  + ' - '+ CAST(ERROR_NUMBER() AS VARCHAR(10)) + ' - ' +  CAST (ERROR_MESSAGE() AS VARCHAR(1000))

                                                                                print @strCommand

                                                                                UPDATE T1

                                                                                SET                         [ErrorDetail]=@ERROR

                                                                                                                ,[StatusID]=@StatusID+1-- Error

 

                                                                                                                FROM [dbo].[vw_ExternalSysDataImport] T1

                                                                                                               

                                                                                WHERE   [StatusID]=3

                                                                                AND EXISTS

                                                                                                                                                                (

                                                                                                                                                                                SELECT  TOP 1 1

                                                                                                                                                                                FROM   [#TEMTABLE] T

                                                                                                                                                                                WHERE                T.[ExternalSysData]=T1.[ID]

                                                                                                                                                                )

                                                                                               

                                                                    EXEC [dbo].[usp_Log_MedSync_ProcessError] @ProcName ,@ERROR ,@StatusID,@AuditId

                                                                                IF OBJECT_ID('tempdb..[#TEMTABLE]') IS NOT NULL DROP TABLE [#TEMTABLE]

                                                               

                                                END CATCH                                                        

                                                                                               

                                                                                               

                END
GO
