-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230330
-- Description:	Обработка данных из [stg].[asto_DataValues_CT] и дальнейщая передача в архиы и хранилище
-- =============================================

CREATE   PROCEDURE [stg].[usp_Etl_asto_Rows_CT]
	@BatchSize INT = 300000, -- Количество записей за цикл (инкрементальная загрузка)
	@DebugInfo INT=1  -- Сохранять в лог подробную информацию
AS
BEGIN
	SET NOCOUNT ON;
	-- Для замеров времени
	DECLARE @TimeStart DATETIME2=SYSDATETIME();
	DECLARE @TimeEnd DATETIME2
	DECLARE @TimeElapsed_s INT

	DECLARE @Message NVARCHAR(1024) -- Переменная для записи в лог

---===================================
	DECLARE @RowCount INT =1
	DECLARE @RowCountAll INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях
	DECLARE @MaxRows INT
	DECLARE @Offset INT = 0
	DECLARE @CycleCount INT =1

	DECLARE @ProcessedTime DATETIME2(3)  -- Переменная для установки времени обработки в staging таблицах

BEGIN TRY
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize

		SET @ProcessedTime=SYSDATETIME()

		;WITH R_CTE
		 AS
		 (
			SELECT R.[RowId]
				  ,R.[__$start_lsn]
				  ,R.[__$end_lsn]
				  ,R.[__$seqval]
				  ,R.[__$operation]
				  ,R.[__$update_mask]
				  ,R.[Id]
				  ,R.[ClosePeriodId]
				  ,R.[ProdDate] --- Добавленные мной поля
				  ,R.[PeriodTypeId] --- Добавленные мной поля
				  ,R.[ReportId] --- Добавленные мной поля     
				  ,R.[ItemId]
				  ,R.[CauseChanged]
				  ---,[DateCreate]  -- //TODO ??? это поле не указано, как отслеживаемое. Надо разобраться
				  ,R.[DateUpdate]
				  ,R.[HistoryUserId]
				  ,R.[__$command_id]
				  ,R.[ErrorStatus]
				  ,R.[ErrorNumber]
				  ,R.[ProcessedTime]
				  ,R.[TranceDate]
				  ,R.[CycleNumber]
				  ,R.[LsnTime]
				  --,CP.[LastDay]
				  --,CP.[PeriodTypeId]
				  --,I.[Id]
		  FROM [stg].[asto_Rows_CT]	R	 
		  WHERE [ErrorStatus] = 0 -- 0 значит не обработанные строки, но буду его обновлять в процессе
		  ORDER BY [RowId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY	
		) 
		UPDATE R
		WITH (TABLOCK)
			SET R.[ProdDate] = CP.[LastDay]
			   ,R.[PeriodTypeId] = CP.[PeriodTypeId]
			   ,R.[ReportId] = ISNULL(I.[ReportId],0)
			   ,R.[ProcessedTime] = @ProcessedTime
			   ,R.[LsnTime] = [ASTO_ZF].sys.fn_cdc_map_lsn_to_time(R.[__$start_lsn])
			   ,R.[CycleNumber] +=(@CycleCount+1000) --debug
			   --- [ErrorNumber] processing
			   ,R.[ErrorNumber] = 
			    (
				IIF([ASTO_ZF].sys.fn_cdc_map_lsn_to_time(R.[__$start_lsn]) IS NULL, POWER(2,1-1), 0) | -- [LsnTime] Не должен быть NULL
				IIF(CP.[LastDay] IS NULL, POWER(2,2-1), 0) | -- [ProdDate] не нашелся (Хотя из-за INNER JOIN такого быть не может)
				IIF(CP.[PeriodTypeId] IS NULL, POWER(2,3-1), 0) | -- [PeriodTypeId] аналогично если два поля выше в ошибке 
				IIF(I.[ReportId] IS NULL, POWER(2,4-1), 0)  -- [ReportId] не шнашелся (не должно быть в принципе) 
				)
		FROM R_CTE R
	  	 LEFT JOIN [ASTO_ZF].[asto].[ClosePeriods] CP ON R.[ClosePeriodId]=CP.[Id]
			LEFT JOIN [ASTO_ZF].[asto].[Items] I ON I.[Id]=R.[ItemId]

		SET @RowCount=@@ROWCOUNT
		SET @RowCountAll+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK


	END -- Конец цикла итераций по @BatchSize

		SET @TimeEnd = SYSDATETIME();
		SET @TimeElapsed_s=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

		IF @RowCountAll>0
			BEGIN
				SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_Rows_CT]. Обработано %i записей в [stg].[asto_Rows_CT]. Затрачено времени - %i сек.',@RowCountAll, @TimeElapsed_s);
				EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
			END
		ELSE
			IF @DebugInfo=1
				BEGIN
					SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_Rows_CT]. Нет записей для обработки в [stg].[asto_Rows_CT] . Затрачено времени - %i сек.', @TimeElapsed_s);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Debug'
				END
		END TRY
		BEGIN CATCH
			SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_Rows_CT]. Ошибка истории CDC. Процедура : %s Ошибка %i : %s', ERROR_PROCEDURE(), ERROR_NUMBER(),ERROR_MESSAGE());
			EXEC [log].[usp_Add_Log_Event] @Message=@Message
		END CATCH

END; -- Конец прцедуры

-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC

-- SELECT * FROM [CDC_DWH].[stg].[asto_Rows_CT]

-- EXECUTE [CDC_DWH].[stg].[usp_Etl_asto_Rows_CT] 	@BatchSize = 300000, @DebugInfo = 1

-- SELECT [CycleNumber], COUNT(*) FROM [CDC_DWH].[stg].[asto_Rows_CT] GROUP BY [CycleNumber]

-- SELECT [ProdDate], COUNT(*) FROM [CDC_DWH].[stg].[asto_Rows_CT] GROUP BY [ProdDate]

-- SELECT [ProcessedTime], COUNT(*) FROM [CDC_DWH].[stg].[asto_Rows_CT] GROUP BY [ProcessedTime]

--UPDATE [CDC_DWH].[stg].[asto_Rows_CT]
--	SET  [ProdDate]='19000131'
--		,[PeriodTypeId]=0
--		,[ReportId]=0
--		,[ErrorNumber]=0
--		,[LsnTime]=NULL
--		,[ProcessedTime]=NULL

 --   SELECT COUNT(*) FROM [stg].[asto_DataValues_CT]
	--SELECT COUNT(*) FROM [ASTO_ZF].[cdc].[asto_DataValues_CT]

 -- SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_Rows_CT]
 -- SELECT COUNT(*) FROM [ASTO_ZF].[cdc].[asto_Rows_CT]