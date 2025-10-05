SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_GetXmlMessage]

(

@ChannelName VARCHAR(100)

)

AS 

                BEGIN 

                INSERT INTO [dbo].[tbl_TempTable] ([KEYFIELD] , [FIELDVALUE])

                VALUES('@ChannelName', @ChannelName);

                                BEGIN 

                                                DECLARE @CurrentDataTime DATETIME;

            SET @CurrentDataTime=GETDATE();

                                                DECLARE @TempTable TABLE ([MSGID] INT ,[MedSyncDataXML] XML)

                                               

                                                INSERT INTO @TempTable ([MSGID],[MedSyncDataXML])

                                                SELECT TOP 1

                                                                ID,

                                                                [MedSyncDataXML]

                                                 FROM   [dbo].[vw_ExternalSysDataImport]

                                                WHERE  [MedSyncDataXML] is not NULL

                                                AND       StatusID=7 --Ready For Convertion

 

 

                                                ------ Update and then Send

                                                UPDATE [dbo].[tbl_ExternalSysDataImport]

                                                SET  StatusID=9

                                                WHERE ID in (SELECT [MSGID] FROM @TempTable )

 

                                                SELECT TOP 1 [MSGID],[MedSyncDataXML]

                                                FROM @TempTable

 

                                END 

                END
GO
