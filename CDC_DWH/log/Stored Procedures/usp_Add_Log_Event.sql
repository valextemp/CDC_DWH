
-- =============================================
-- Author:		Alekseenko V.V.
-- Create date:	20230411
-- Description:	Процедура добавления записи в журнал событий (логирование)
-- =============================================
CREATE PROCEDURE [log].[usp_Add_Log_Event] (
	@Level [nvarchar](32)=N'Erorr',
	@Source [nvarchar](100)=N'CDC_DWH',
	@UserName [nvarchar](100)=NULL,
	@Category [nvarchar](32)=N'History',
	@Message [nvarchar](1024),
	@Details [xml] = NULL
)
AS
BEGIN
	SET NOCOUNT ON;

	BEGIN TRY
		INSERT INTO [log].[Events] ([TimeEvent], [Level], [Source], [Category], [UserName], [Message], [Details])
		VALUES (
			SYSDATETIME() -- [TimeEvent]
			,@Level -- [Level]
			,@Source -- [Source]
			,@Category -- [Category]
			,N'N/A' -- [UserName]
			,@Message --[Message]
			,@Details -- [Details]
			);
	END TRY
	BEGIN CATCH
	END CATCH

END;