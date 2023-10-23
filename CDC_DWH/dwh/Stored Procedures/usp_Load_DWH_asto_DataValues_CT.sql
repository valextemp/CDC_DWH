-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230422
-- Description:	Обработка данных из [stg].[asto_DataValues_CT] и дальнейщая передача в архиы и хранилище
-- =============================================

CREATE   PROCEDURE [dwh].[usp_Load_DWH_asto_DataValues_CT]
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
	DECLARE @RowCountAll INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [stg].[asto_DataValues_CT]
	DECLARE @RowCountAllArh INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [arсh].[asto_DataValues_CT]
	DECLARE @RowCountAllPT_Free INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [dwh].[asto_DataValues_CT_PT_free]
	DECLARE @MaxRows INT
	DECLARE @Offset INT = 0
	DECLARE @CycleCount INT =1

BEGIN TRY
---=================================================================================
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT]
	MERGE [dwh].[asto_DataValues_CT] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			   DVal.[ProdDate]  --- Добавленные мной поля
			  ,DVal.[__$start_lsn]
			  ,DVal.[__$end_lsn]
			  ,DVal.[__$seqval]
			  ,DVal.[__$operation]
			  ,DVal.[__$update_mask]
			  ,DVal.[Id]
			  ,DVal.[ClosePeriodId]
			  ,DVal.[PeriodTypeId] --- Добавленные мной поля
			  ,DVal.[ReportId] --- Добавленные мной поля   
			  ,DVal.[ItemId]
			  ,DVal.[RowId]
			  ,DVal.[ElementId]
			  ,DVal.[DataTypeId]
			  ,DVal.[UnitId]
			  ,DVal.[Value]
			  ,DVal.[CauseChanged]
			  ,DVal.[DateCreate]
			  ,DVal.[DateUpdate]
			  ,DVal.[ShipConsignmentId]
			  ,DVal.[DataSourceCellId]
			  ,DVal.[HistoryUserId]
			  ,DVal.[__$command_id]
			  ,DVal.[IsValueChanged]
			  ,DVal.[ErrorStatus]
			  ,DVal.[ErrorNumber]
			  ,DVal.[ProcessedTime]
			  ,DVal.[TranceDate]
			  ,DVal.[CycleNumber]
			  ,DVal.[LsnTime]
		  FROM [stg].[asto_DataValues_CT] DVal	 
		  WHERE [IsValueChanged] = 1 -- Только где менялось поле [Value]
		  ORDER BY DVal.[DvId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
		INSERT ([ProdDate]	  -- DVal.[ProdDate]  --- Добавленные мной поля
			   ,[__$start_lsn]	  -- DVal.[__$start_lsn]
			   ,[__$end_lsn]	  -- DVal.[__$end_lsn]
			   ,[__$seqval]	  -- DVal.[__$seqval]
			   ,[__$operation]	  -- DVal.[__$operation]
			   ,[__$update_mask]	  -- DVal.[__$update_mask]
			   ,[Id]	  -- DVal.[Id]
			   ,[ClosePeriodId]	  -- DVal.[ClosePeriodId]
			   ,[PeriodTypeId]	  -- DVal.[PeriodTypeId] --- Добавленные мной поля
			   ,[ReportId]	  -- DVal.[ReportId] --- Добавленные мной поля   
			   ,[ItemId]	  -- DVal.[ItemId]
			   ,[RowId]	  -- DVal.[RowId]
			   ,[ElementId]	  -- DVal.[ElementId]
			   ,[DataTypeId]	  -- DVal.[DataTypeId]
			   ,[UnitId]	  -- DVal.[UnitId]
			   ,[Value]	  -- DVal.[Value]
			   ,[CauseChanged]	  -- DVal.[CauseChanged]
			   ,[DateCreate]	  -- DVal.[DateCreate]
			   ,[DateUpdate]	  -- DVal.[DateUpdate]
			   ,[ShipConsignmentId]	  -- DVal.[ShipConsignmentId]
			   ,[DataSourceCellId]	  -- DVal.[DataSourceCellId]
			   ,[HistoryUserId]	  -- DVal.[HistoryUserId]
			   ,[__$command_id]	  -- DVal.[__$command_id]
			   ,[IsValueChanged]	  -- DVal.[IsValueChanged]
			   ,[ErrorStatus]	  -- DVal.[ErrorStatus]
			   ,[ErrorNumber]	  -- DVal.[ErrorNumber]
			   ,[ProcessedTime]	  -- DVal.[ProcessedTime]
			   ,[TranceDate]	  -- DVal.[TranceDate]
			   ,[CycleNumber]	  -- DVal.[CycleNumber]
			   ,[LsnTime])	  -- DVal.[LsnTime]
		   VALUES 
			   (SRC.[ProdDate]  --- Добавленные мной поля
			  ,SRC.[__$start_lsn]
			  ,SRC.[__$end_lsn]
			  ,SRC.[__$seqval]
			  ,SRC.[__$operation]
			  ,SRC.[__$update_mask]
			  ,SRC.[Id]
			  ,SRC.[ClosePeriodId]
			  ,SRC.[PeriodTypeId] --- Добавленные мной поля
			  ,SRC.[ReportId] --- Добавленные мной поля   
			  ,SRC.[ItemId]
			  ,SRC.[RowId]
			  ,SRC.[ElementId]
			  ,SRC.[DataTypeId]
			  ,SRC.[UnitId]
			  ,SRC.[Value]
			  ,SRC.[CauseChanged]
			  ,SRC.[DateCreate]
			  ,SRC.[DateUpdate]
			  ,SRC.[ShipConsignmentId]
			  ,SRC.[DataSourceCellId]
			  ,SRC.[HistoryUserId]
			  ,SRC.[__$command_id]
			  ,SRC.[IsValueChanged]
			  ,SRC.[ErrorStatus]
			  ,SRC.[ErrorNumber]
			  ,SRC.[ProcessedTime]
			  ,SRC.[TranceDate]
			  ,@CycleCount  -- SRC.[CycleNumber]
			  ,SRC.[LsnTime]);

		SET @RowCount=@@ROWCOUNT
--/*debug*/		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAll+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT]
---=================================================================================
-- Еще один цикл вставки в несекционированную таблицу
	SET @RowCount=1
	SET @Offset = 0
	SET @CycleCount = 1
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT_PT_free]
	MERGE [dwh].[asto_DataValues_CT_PT_free] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			   DVal.[ProdDate]  --- Добавленные мной поля
			  ,DVal.[__$start_lsn]
			  ,DVal.[__$end_lsn]
			  ,DVal.[__$seqval]
			  ,DVal.[__$operation]
			  ,DVal.[__$update_mask]
			  ,DVal.[Id]
			  ,DVal.[ClosePeriodId]
			  ,DVal.[PeriodTypeId] --- Добавленные мной поля
			  ,DVal.[ReportId] --- Добавленные мной поля   
			  ,DVal.[ItemId]
			  ,DVal.[RowId]
			  ,DVal.[ElementId]
			  ,DVal.[DataTypeId]
			  ,DVal.[UnitId]
			  ,DVal.[Value]
			  ,DVal.[CauseChanged]
			  ,DVal.[DateCreate]
			  ,DVal.[DateUpdate]
			  ,DVal.[ShipConsignmentId]
			  ,DVal.[DataSourceCellId]
			  ,DVal.[HistoryUserId]
			  ,DVal.[__$command_id]
			  ,DVal.[IsValueChanged]
			  ,DVal.[ErrorStatus]
			  ,DVal.[ErrorNumber]
			  ,DVal.[ProcessedTime]
			  ,DVal.[TranceDate]
			  ,DVal.[CycleNumber]
			  ,DVal.[LsnTime]
		  FROM [stg].[asto_DataValues_CT] DVal	 
		  WHERE [IsValueChanged] = 1 -- Только где менялось поле [Value]
		  ORDER BY DVal.[DvId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
		INSERT ([ProdDate]	  -- DVal.[ProdDate]  --- Добавленные мной поля
			   ,[__$start_lsn]	  -- DVal.[__$start_lsn]
			   ,[__$end_lsn]	  -- DVal.[__$end_lsn]
			   ,[__$seqval]	  -- DVal.[__$seqval]
			   ,[__$operation]	  -- DVal.[__$operation]
			   ,[__$update_mask]	  -- DVal.[__$update_mask]
			   ,[Id]	  -- DVal.[Id]
			   ,[ClosePeriodId]	  -- DVal.[ClosePeriodId]
			   ,[PeriodTypeId]	  -- DVal.[PeriodTypeId] --- Добавленные мной поля
			   ,[ReportId]	  -- DVal.[ReportId] --- Добавленные мной поля   
			   ,[ItemId]	  -- DVal.[ItemId]
			   ,[RowId]	  -- DVal.[RowId]
			   ,[ElementId]	  -- DVal.[ElementId]
			   ,[DataTypeId]	  -- DVal.[DataTypeId]
			   ,[UnitId]	  -- DVal.[UnitId]
			   ,[Value]	  -- DVal.[Value]
			   ,[CauseChanged]	  -- DVal.[CauseChanged]
			   ,[DateCreate]	  -- DVal.[DateCreate]
			   ,[DateUpdate]	  -- DVal.[DateUpdate]
			   ,[ShipConsignmentId]	  -- DVal.[ShipConsignmentId]
			   ,[DataSourceCellId]	  -- DVal.[DataSourceCellId]
			   ,[HistoryUserId]	  -- DVal.[HistoryUserId]
			   ,[__$command_id]	  -- DVal.[__$command_id]
			   ,[IsValueChanged]	  -- DVal.[IsValueChanged]
			   ,[ErrorStatus]	  -- DVal.[ErrorStatus]
			   ,[ErrorNumber]	  -- DVal.[ErrorNumber]
			   ,[ProcessedTime]	  -- DVal.[ProcessedTime]
			   ,[TranceDate]	  -- DVal.[TranceDate]
			   ,[CycleNumber]	  -- DVal.[CycleNumber]
			   ,[LsnTime])	  -- DVal.[LsnTime]
		   VALUES 
			   (SRC.[ProdDate]  --- Добавленные мной поля
			  ,SRC.[__$start_lsn]
			  ,SRC.[__$end_lsn]
			  ,SRC.[__$seqval]
			  ,SRC.[__$operation]
			  ,SRC.[__$update_mask]
			  ,SRC.[Id]
			  ,SRC.[ClosePeriodId]
			  ,SRC.[PeriodTypeId] --- Добавленные мной поля
			  ,SRC.[ReportId] --- Добавленные мной поля   
			  ,SRC.[ItemId]
			  ,SRC.[RowId]
			  ,SRC.[ElementId]
			  ,SRC.[DataTypeId]
			  ,SRC.[UnitId]
			  ,SRC.[Value]
			  ,SRC.[CauseChanged]
			  ,SRC.[DateCreate]
			  ,SRC.[DateUpdate]
			  ,SRC.[ShipConsignmentId]
			  ,SRC.[DataSourceCellId]
			  ,SRC.[HistoryUserId]
			  ,SRC.[__$command_id]
			  ,SRC.[IsValueChanged]
			  ,SRC.[ErrorStatus]
			  ,SRC.[ErrorNumber]
			  ,SRC.[ProcessedTime]
			  ,SRC.[TranceDate]
			  ,@CycleCount  -- SRC.[CycleNumber]
			  ,SRC.[LsnTime]);

		SET @RowCount=@@ROWCOUNT
--/*debug*/		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAllPT_Free+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT_PT_free]

---=================================================================================
-- Новый цикл инкрементальной вставки в [arсh].[asto_DataValues_CT]
	SET @RowCount=1
	SET @Offset = 0
	SET @CycleCount = 1
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [arсh].[asto_DataValues_CT]
	MERGE [arсh].[asto_DataValues_CT] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			   DVal.[__$start_lsn]
			  ,DVal.[__$end_lsn]
			  ,DVal.[__$seqval]
			  ,DVal.[__$operation]
			  ,DVal.[__$update_mask]
			  ,DVal.[Id]
			  ,DVal.[ClosePeriodId]
			  ,DVal.[RowId]
			  ,DVal.[ElementId]
			  ,DVal.[DataTypeId]
			  ,DVal.[UnitId]
			  ,DVal.[Value]
			  ,DVal.[DateCreate]
			  ,DVal.[DateUpdate]
			  ,DVal.[ShipConsignmentId]
			  ,DVal.[DataSourceCellId]
			  ,DVal.[HistoryUserId]
			  ,DVal.[__$command_id]
			  ,DVal.[IsValueChanged]
			  ,DVal.[ErrorNumber]
			  ,DVal.[ProcessedTime]
			  ,DVal.[TranceDate]
			  ,DVal.[CycleNumber]
			  ,DVal.[LsnTime]
		  FROM [stg].[asto_DataValues_CT] DVal	 
		  ORDER BY DVal.[DvId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
	INSERT
		   (
		    [__$start_lsn]	   --- SRC.[__$start_lsn]	
           ,[__$end_lsn]	   --- SRC.[__$end_lsn]	
           ,[__$seqval]	   --- SRC.[__$seqval]	
           ,[__$operation]	   --- SRC.[__$operation]	
           ,[__$update_mask]	   --- SRC.[__$update_mask]	
           ,[Id]	   --- SRC.[Id]	
           ,[ClosePeriodId]	   --- SRC.[ClosePeriodId]	
           ,[RowId]	   --- SRC.[RowId]	
           ,[ElementId]	   --- SRC.[ElementId]	
           ,[DataTypeId]	   --- SRC.[DataTypeId]	
           ,[UnitId]	   --- SRC.[UnitId]	
           ,[Value]	   --- SRC.[Value]	
           ,[DateCreate]	   --- SRC.[DateCreate]	
           ,[DateUpdate]	   --- SRC.[DateUpdate]	
           ,[ShipConsignmentId]	   --- SRC.[ShipConsignmentId]	
           ,[DataSourceCellId]	   --- SRC.[DataSourceCellId]	
           ,[HistoryUserId]	   --- SRC.[HistoryUserId]	
           ,[__$command_id]	   --- SRC.[__$command_id]	
           ,[IsValueChanged]	   --- SRC.[IsValueChanged]	
           ,[ErrorNumber]	   --- SRC.[ErrorNumber]	
           ,[ProcessedTime]	   --- SRC.[ProcessedTime]	
           ,[TranceDate]	   --- SRC.[TranceDate]	
           ,[CycleNumber]	   --- @CycleCount  -- SRC.[CycleNumber]	
           ,[LsnTime]	   --- SRC.[LsnTime]);	
		   )
	VALUES
		(
		  SRC.[__$start_lsn]
		  ,SRC.[__$end_lsn]
		  ,SRC.[__$seqval]
		  ,SRC.[__$operation]
		  ,SRC.[__$update_mask]
		  ,SRC.[Id]
		  ,SRC.[ClosePeriodId]
		  ,SRC.[RowId]
		  ,SRC.[ElementId]
		  ,SRC.[DataTypeId]
		  ,SRC.[UnitId]
		  ,SRC.[Value]
		  ,SRC.[DateCreate]
		  ,SRC.[DateUpdate]
		  ,SRC.[ShipConsignmentId]
		  ,SRC.[DataSourceCellId]
		  ,SRC.[HistoryUserId]
		  ,SRC.[__$command_id]
		  ,SRC.[IsValueChanged]
		  ,SRC.[ErrorNumber]
		  ,SRC.[ProcessedTime]
		  ,SRC.[TranceDate]
		  ,@CycleCount  -- ,SRC.[CycleNumber]
		  ,SRC.[LsnTime]
		);

		SET @RowCount=@@ROWCOUNT
--/*debug*/		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAllArh+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT]

		SET @TimeEnd = SYSDATETIME();
		SET @TimeElapsed_s=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

		IF @RowCountAll>0 OR @RowCountAllPT_Free>0 OR @RowCountAllArh>0
			BEGIN
				SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_DataValues_CT]. Записано %i строк в [dwh].[asto_DataValues_CT]. Записано %i строк в [arсh].[asto_DataValues_CT]. Затрачено времени - %i сек.',@RowCountAll, @RowCountAllArh, @TimeElapsed_s);
				EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
			END
		ELSE
			IF @DebugInfo=1
				BEGIN
					SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_DataValues_CT]. Нет записей в [stg].[asto_DataValues_CT] . Затрачено времени - %i сек.', @TimeElapsed_s);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Debug'
				END

---=======================================
-- Блок удаления данных из [stg] таблицы
	TRUNCATE TABLE [stg].[asto_DataValues_CT]
---=======================================

-- /*debug*/	SELECT CONCAT(N'Вставлено - ', @RowCountAll, ' / в [dwh].[asto_DataValues_CT]'), CONCAT(N'Вставлено - ', @RowCountAllPT_Free, ' / в [dwh].[asto_DataValues_CT_PT_Free]'), CONCAT(N'Вставлено - ', @RowCountAllArh, ' / в [arсh].[asto_DataValues_CT]')

		END TRY
		BEGIN CATCH
			SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_DataValues_CT]. Ошибка истории CDC. Процедура : %s Ошибка %i : %s', ERROR_PROCEDURE(), ERROR_NUMBER(),ERROR_MESSAGE());
			EXEC [log].[usp_Add_Log_Event] @Message=@Message
		END CATCH

END;

---=========================================================
-- Testing

-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC
-- EXECUTE [CDC_DWH].[dwh].[usp_Load_DWH_asto_DataValues_CT] 	@BatchSize = 10, @DebugInfo = 1  -- Сохранять в лог подробную информацию

 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_DataValues_CT]
 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_DataValues_CT_PT_free]
 --TRUNCATE TABLE [CDC_DWH].[arсh].[asto_DataValues_CT]
 --TRUNCATE TABLE [CDC_DWH].[stg].[asto_DataValues_CT]

-- TRUNCATE TABLE [CDC_DWH].[log].[Events]

-- SELECT * FROM [CDC_DWH].[dwh].[asto_DataValues_CT]
---- SELECT * FROM [stg].[asto_DataValues_CT] WHERE [IsValueChanged] = 1 

-- SELECT * FROM [CDC_DWH].[dwh].[asto_DataValues_CT_PT_free]
-- SELECT * FROM [CDC_DWH].[arсh].[asto_DataValues_CT]

-- SELECT [__$start_lsn], [__$seqval],[__$operation], COUNT (*) num FROM [stg].[asto_DataValues_CT]
-- GROUP BY [__$start_lsn], [__$seqval],[__$operation]
-- ORDER BY Num desc