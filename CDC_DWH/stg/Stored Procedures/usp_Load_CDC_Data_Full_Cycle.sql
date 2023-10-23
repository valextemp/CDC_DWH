-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230330
-- Description:	Загрузка данных из asto_DataValues_CT в [stg].[asto_DataValues_CT]
-- =============================================

CREATE   PROCEDURE [stg].[usp_Load_CDC_Data_Full_Cycle]
	@BatchSize INT = 1000000, -- Количество записей за цикл (инкрементальная загрузка)
	@DebugInfo INT=1  -- Сохранять в лог подробную информацию
AS
BEGIN
	SET NOCOUNT ON;

	-- Для замеров времени
	DECLARE @TimeStart DATETIME2=SYSDATETIME();
	DECLARE @TimeEnd DATETIME2
	DECLARE @TimeElapsed_sek INT

	DECLARE @Message NVARCHAR(1024) -- Переменная для записи в лог

	BEGIN -- Начало тела процедуры [stg].[usp_Load_CDC_Data_Full_Cycle]
	
		SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
			BEGIN TRY
					EXECUTE [stg].[usp_Load_asto_DataValues_CT] @BatchSize=@BatchSize, @DebugInfo=@DebugInfo
					EXECUTE [stg].[usp_Load_asto_Rows_CT] @BatchSize=@BatchSize, @DebugInfo=@DebugInfo
	
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED
					EXECUTE [stg].[usp_Etl_asto_DataValues_CT] @BatchSize = @BatchSize, @DebugInfo = @DebugInfo
					EXECUTE [stg].[usp_Etl_asto_Rows_CT] @BatchSize = @BatchSize, @DebugInfo = @DebugInfo

					EXECUTE [dwh].[usp_Load_DWH_asto_DataValues_CT] @BatchSize = @BatchSize, @DebugInfo = @DebugInfo  -- Сохранять в лог подробную информацию
					EXECUTE [dwh].[usp_Load_DWH_asto_Rows_CT] @BatchSize = @BatchSize, @DebugInfo = @DebugInfo  -- Сохранять в лог подробную информацию

					SET @TimeEnd = SYSDATETIME(); 
					SET @TimeElapsed_sek=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

					SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Load_CDC_Data_Full_Cycle]. Общее время перекладки/обработки CDC данных - %i секунд.', @TimeElapsed_sek);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
		
		END TRY
		BEGIN CATCH
			SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Load_CDC_Data_Full_Cycle]. Ошибка истории CDC. Процедура : %s Ошибка %i : %s', ERROR_PROCEDURE(), ERROR_NUMBER(),ERROR_MESSAGE());
			EXEC [log].[usp_Add_Log_Event] @Message=@Message
		END CATCH

	END -- Конец тела процедуры [stg].[usp_Load_CDC_Data_Full_Cycle]

END

-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC

-- EXECUTE [CDC_DWH].[stg].[usp_Load_CDC_Data_Full_Cycle] @BatchSize = 10, @DebugInfo = 1

 --TRUNCATE TABLE [CDC_DWH].[stg].[asto_DataValues_CT]
 --TRUNCATE TABLE [CDC_DWH].[stg].[asto_Rows_CT]
 --TRUNCATE TABLE [CDC_DWH].[log].[Events]

 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_DataValues_CT]
 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_DataValues_CT_PT_free]
 --TRUNCATE TABLE [CDC_DWH].[arсh].[asto_DataValues_CT]
 --TRUNCATE TABLE [CDC_DWH].[stg].[asto_DataValues_CT]

 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_Rows_CT]
 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_Rows_CT_PT_free]
 --TRUNCATE TABLE [CDC_DWH].[arсh].[asto_Rows_CT]



--SELECT o.name AS TableName, partition_id AS PartitionID, partition_number AS PartitionNumber, Rows , '------', p.*,'------', o.*
--FROM sys.partitions p 
--INNER JOIN sys.objects o ON o.object_id = p.object_id 
--WHERE o.name = 'asto_Rows_CT' 
--ORDER BY partition_number; 
--GO 