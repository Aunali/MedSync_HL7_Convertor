SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[usp_DIC_ExternalSysFieldMapping_UI]
(
@InputXML VARCHAR(MAX)
)
AS 
                BEGIN 
				
			BEGIN TRY
					
			DECLARE @PayLoad XML ,
			@QueryString varchar(1000),
			@ReturnType VARCHAR (10),
			@ID INT,
			@ExternalSystemID INT,
			@MedSyncPayload nvarchar(max),
		    @MedSyncPayloadXML xml
			;


		SELECT @QueryString=[MedSyncConnector].[dbo].[udf_GetQueryString](@InputXML)

		
		
		EXEC  [MedSyncConnector].[dbo].[usp_Debug_Log] 'usp_DIC_ExternalSysFieldMapping_UI:InputXml',@InputXml
		EXEC  [MedSyncConnector].[dbo].[usp_Debug_Log] 'usp_DIC_ExternalSysFieldMapping_UI:QueryString',@QueryString 

		SELECT @ID =  [MedSyncConnector].[dbo].[udf_GetINTValue]([MedSyncConnector].[dbo].[udf_GetQueryParamByName](@InputXml,'ID'))

		SELECT @ExternalSystemID =  [MedSyncConnector].[dbo].[udf_GetINTValue]([MedSyncConnector].[dbo].[udf_GetQueryParamByName](@InputXml,'ExternalSystemID'))
		SELECT	@ReturnType		= [MedSyncConnector].[dbo].[udf_GetReturnType](@InputXml)

	--	Select @ExternalSystemID


		IF (@ReturnType='XML')

			BEGIN
		Print 'I am Here XML'

		SET @MedSyncPayloadXML =(
							SELECT
								(
									SELECT 
											 [ID]
											,[HL7Segment]
											,[ExternalSystemID]
											,[FieldName]
											,[XQueryPath]
											,[DefaultValue]
											,[CreatedDate]
											,[UpdatedDate]
											,[UDFFunction]
											,[SEQNO]
											,[Position]
									FROM	[dbo].[vw_DIC_ExternalSysFieldMapping]
									WHERE	1=1
									AND  
											(
													[ID]=@ID 
													  OR
													@ID=0
											)
									AND 
											(

												[ExternalSystemID]=@ExternalSystemID 
														OR
												@ExternalSystemID=0
											)
									ORDER BY [SEQNO]
									FOR XML PATH('MedSync_HL7_Convertor'), TYPE
								) AS [Applications]
								FOR XML PATH('MedSyncPayload'), TYPE
								);
				SET @MedSyncPayload =CAST(@MedSyncPayloadXML AS NVARCHAR(MAX));
			END
			ELSE IF (@ReturnType='JSON')
				BEGIN
				Print 'I am Here XML'
						SELECT @MedSyncPayload =
						(
							SELECT
								JSON_QUERY(
									(
											SELECT 
													 [ID]
													,[HL7Segment]
													,[ExternalSystemID]
													,[FieldName]
													,[XQueryPath]
													,[DefaultValue]
													,[CreatedDate]
													,[UpdatedDate]
													,[UDFFunction]
													,[SEQNO]
													,[Position]
											FROM	[dbo].[vw_DIC_ExternalSysFieldMapping]
											WHERE	1=1
											AND  
													(
															[ID]=@ID 
															  OR
															@ID=0
													)
											AND 
													(

														[ExternalSystemID]=@ExternalSystemID 
																OR
														@ExternalSystemID=0
													)
											ORDER BY [SEQNO]
										FOR JSON PATH
									)
								) AS MedSync_HL7_Convertor
							FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
						);
					END 

-- use it
			SELECT @MedSyncPayload AS MedSyncPayload;
	

		END TRY 
		BEGIN CATCH

		Print ' I am In the Catch'
		 DECLARE
            @ErrNumber     INT             = ERROR_NUMBER(),
            @ErrSeverity   INT             = ERROR_SEVERITY(),
            @ErrState      INT             = ERROR_STATE(),
            @ErrLine       INT             = ERROR_LINE(),
            @ErrProcedure  SYSNAME         = ERROR_PROCEDURE(),
            @ErrMessage    NVARCHAR(2048)  = ERROR_MESSAGE();

        ----------------------------------------------------------------
        -- Build XML error payload (elements + some attributes)
        ----------------------------------------------------------------
		IF (@ReturnType='XML')

			BEGIN

        SET @MedSyncPayloadXML =
        (
            SELECT
                'AAA.NET.MedSyncAPI'              AS [Source],
                SYSDATETIMEOFFSET()               AS [Timestamp],
                'Error'                           AS [Status],
                @ErrNumber                        AS [Error/@Number],
                @ErrSeverity                      AS [Error/@Severity],
                @ErrState                         AS [Error/@State],
                @ErrLine                          AS [Error/@Line],
                ISNULL(@ErrProcedure, '')         AS [Error/@Procedure],
                @ErrMessage                       AS [Error/Message],
                TRY_CAST(@InputXml AS XML)        AS [Context/Input]
            FOR XML PATH('MedSync_HL7_Convertor'), TYPE, ROOT('MedSyncPayload')
        );

							
				SET @MedSyncPayload =CAST(@MedSyncPayloadXML AS NVARCHAR(MAX));

END 

ELSE IF (@ReturnType='JSON')
				BEGIN
		SET @MedSyncPayload =
(
    SELECT
        (
            SELECT
                'AAA.NET.MedSyncAPI'              AS [Source],
                SYSDATETIMEOFFSET()               AS [Timestamp],
                'Error'                           AS [Status],
                @ErrNumber                        AS [Error.Number],
                @ErrSeverity                      AS [Error.Severity],
                @ErrState                         AS [Error.State],
                @ErrLine                          AS [Error.Line],
                ISNULL(@ErrProcedure,'')          AS [Error.Procedure],
                @ErrMessage                       AS [Error.Message],
                CAST(@InputXml AS nvarchar(max))  AS [Context.Input]      -- keep XML as string
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
        ) AS [MedSyncPayload.MedSync_HL7_Convertor]
    FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
);

	SELECT @MedSyncPayload AS MedSyncPayload;
END
		END CATCH

        END
GO
