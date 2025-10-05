SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_DIC_ExternalSys_UI]
(
@InputXML VARCHAR(MAX)
)
AS 
                BEGIN 
				SELECT 
						 [ExternalSystemID] 				,[SystemName] 				,[Active]
						,[ProcedureToExecute]				,[CreatedDate]				,[UpdatedDate]
						,[SeqNo]							,[Link]						,[RootElement]
						,[Filter]							,[FileNameContain] 			,[SourceID]
						,[UseColumnName]					,[FirstRowHeader]			
				 FROM [dbo].[vw_DIC_ExternalSys]
                END
GO
