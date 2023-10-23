-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230530
-- Description:	Копирование данных из [cdc].[asto_DataValues_CT] и [cdc].[asto_Rows_CT] на всякий случай
-- =============================================


CREATE       PROCEDURE [stg].[usp_Copy_CDC_Data]
AS
BEGIN
	SET NOCOUNT ON;
	-- Для замеров времени
	DECLARE @TimeStart DATETIME2=SYSDATETIME();
	DECLARE @TimeEnd DATETIME2
	DECLARE @TimeElapsed_s INT

	DECLARE @Message NVARCHAR(1024) -- Переменная для записи в лог

	DECLARE @TransDate DATE = CAST(GETDATE() AS DATE)
	DECLARE @TransDateStr NVARCHAR(8) = FORMAT(@TransDate,'yyyyMMdd','ru-ru')
	DECLARE @RowCount_DV INT
	DECLARE @RowCount_Rows INT

	--DECLARE @SQL_DV NVARCHAR(MAX)=N'DROP TABLE IF EXISTS [CDC_DWH].[dbo].[asto_DataValues_CT_copy_'+@TransDateStr+']
	--SELECT *
	--INTO [CDC_DWH].[dbo].[asto_DataValues_CT_copy_'+@TransDateStr+']
	--FROM [ASTO_ZF].[cdc].[asto_DataValues_CT]'

	--EXEC (@SQL_DV)
	--SET @RowCount_DV = @@ROWCOUNT

	--DECLARE @SQL_Rows NVARCHAR(MAX)=N'DROP TABLE IF EXISTS [CDC_DWH].[dbo].[asto_Rows_CT_copy_'+@TransDateStr+']
	--SELECT * 
	--INTO [CDC_DWH].[dbo].[asto_Rows_CT_copy_'+@TransDateStr+']
	--FROM [ASTO_ZF].[cdc].[asto_Rows_CT]'

	--EXEC (@SQL_Rows)
	--SET @RowCount_Rows = @@ROWCOUNT

	--SET @TimeEnd = SYSDATETIME();
	--SET @TimeElapsed_s=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

	--SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Copy_CDC_Data]. Вставлено - %i записей в [dbo].[asto_DataValues_CT_%s] и - %i записей в [dbo].[asto_Rows_CT_copy_%s]. Затрачено времени - %i сек.', ISNULL(@RowCount_DV,0), @TransDateStr, ISNULL(@RowCount_Rows,0), @TransDateStr ,  @TimeElapsed_s);
	--EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'

END

-- EXECUTE [stg].[usp_Copy_CDC_Data]