-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230330
-- Description:	Обработка данных из [stg].[asto_Rows_CT] и дальнейщая передача в архиы и хранилище
-- =============================================

CREATE   PROCEDURE [dwh].[usp_Load_DWH_asto_Rows_CT]
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
	DECLARE @RowCountAll INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [dwh].[asto_Rows_CT]
	DECLARE @RowCountAllArh INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [arсh].[asto_Rows_CT]
	DECLARE @RowCountAllPT_Free INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях в [dwh].[asto_Rows_CT_PT_free]
	DECLARE @MaxRows INT
	DECLARE @Offset INT = 0
	DECLARE @CycleCount INT =1

	DECLARE @ProcessedTime DATETIME2(3)  -- Переменная для установки времени обработки в staging таблицах

BEGIN TRY
---=================================================================================
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [dwh].[asto_DataValues_CT]
	MERGE [dwh].[asto_Rows_CT] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			R.[__$start_lsn]
           ,R.[__$end_lsn]
           ,R.[__$seqval]
           ,R.[__$operation]
           ,R.[__$update_mask]
           ,R.[Id]
           ,R.[ClosePeriodId]
           ,R.[ProdDate]
           ,R.[PeriodTypeId]
           ,R.[ReportId]
           ,R.[ItemId]
           ,R.[CauseChanged]
           ,R.[DateCreate]
           ,R.[DateUpdate]
           ,R.[HistoryUserId]
           ,R.[__$command_id]
           ,R.[ErrorStatus]
           ,R.[ErrorNumber]
           ,R.[ProcessedTime]
           ,R.[TranceDate]
           ,R.[CycleNumber]
           ,R.[LsnTime]
		  FROM [stg].[asto_Rows_CT] R 
		  ORDER BY R.[RowId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
		INSERT (
					[__$start_lsn]		           --- R.[__$start_lsn]
				   ,[__$end_lsn]		           --- R.[__$end_lsn]
				   ,[__$seqval]		           --- R.[__$seqval]
				   ,[__$operation]		           --- R.[__$operation]
				   ,[__$update_mask]		           --- R.[__$update_mask]
				   ,[Id]		           --- R.[Id]
				   ,[ClosePeriodId]		           --- R.[ClosePeriodId]
				   ,[ProdDate]		           --- R.[ProdDate]
				   ,[PeriodTypeId]		           --- R.[PeriodTypeId]
				   ,[ReportId]		           --- R.[ReportId]
				   ,[ItemId]		           --- R.[ItemId]
				   ,[CauseChanged]		           --- R.[CauseChanged]
				   ,[DateCreate]		           --- R.[DateCreate]
				   ,[DateUpdate]		           --- R.[DateUpdate]
				   ,[HistoryUserId]		           --- R.[HistoryUserId]
				   ,[__$command_id]		           --- R.[__$command_id]
				   ,[ErrorStatus]		           --- R.[ErrorStatus]
				   ,[ErrorNumber]		           --- R.[ErrorNumber]
				   ,[ProcessedTime]		           --- R.[ProcessedTime]
				   ,[TranceDate]		           --- R.[TranceDate]
				   ,[CycleNumber]		           --- R.[CycleNumber]
				   ,[LsnTime]		           --- R.[LsnTime]
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
				   ,SRC.[ProdDate]
				   ,SRC.[PeriodTypeId]
				   ,SRC.[ReportId]
				   ,SRC.[ItemId]
				   ,SRC.[CauseChanged]
				   ,SRC.[DateCreate]
				   ,SRC.[DateUpdate]
				   ,SRC.[HistoryUserId]
				   ,SRC.[__$command_id]
				   ,SRC.[ErrorStatus]
				   ,SRC.[ErrorNumber]
				   ,SRC.[ProcessedTime]
				   ,SRC.[TranceDate]
				   ,@CycleCount -- SRC.[CycleNumber]
				   ,SRC.[LsnTime]
			  );

		SET @RowCount=@@ROWCOUNT
--		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAll+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [dwh].[asto_Rows_CT]
---=================================================================================
-- Еще один цикл вставки в несекционированную таблицу
	SET @RowCount=1
	SET @Offset = 0
	SET @CycleCount = 1
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [dwh].[asto_Rows_CT_PT_free]
	MERGE [dwh].[asto_Rows_CT_PT_free] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			R.[__$start_lsn]
           ,R.[__$end_lsn]
           ,R.[__$seqval]
           ,R.[__$operation]
           ,R.[__$update_mask]
           ,R.[Id]
           ,R.[ClosePeriodId]
           ,R.[ProdDate]
           ,R.[PeriodTypeId]
           ,R.[ReportId]
           ,R.[ItemId]
           ,R.[CauseChanged]
           ,R.[DateCreate]
           ,R.[DateUpdate]
           ,R.[HistoryUserId]
           ,R.[__$command_id]
           ,R.[ErrorStatus]
           ,R.[ErrorNumber]
           ,R.[ProcessedTime]
           ,R.[TranceDate]
           ,R.[CycleNumber]
           ,R.[LsnTime]
		  FROM [stg].[asto_Rows_CT] R 
		  ORDER BY R.[RowId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
		INSERT (
					[__$start_lsn]		           --- R.[__$start_lsn]
				   ,[__$end_lsn]		           --- R.[__$end_lsn]
				   ,[__$seqval]		           --- R.[__$seqval]
				   ,[__$operation]		           --- R.[__$operation]
				   ,[__$update_mask]		           --- R.[__$update_mask]
				   ,[Id]		           --- R.[Id]
				   ,[ClosePeriodId]		           --- R.[ClosePeriodId]
				   ,[ProdDate]		           --- R.[ProdDate]
				   ,[PeriodTypeId]		           --- R.[PeriodTypeId]
				   ,[ReportId]		           --- R.[ReportId]
				   ,[ItemId]		           --- R.[ItemId]
				   ,[CauseChanged]		           --- R.[CauseChanged]
				   ,[DateCreate]		           --- R.[DateCreate]
				   ,[DateUpdate]		           --- R.[DateUpdate]
				   ,[HistoryUserId]		           --- R.[HistoryUserId]
				   ,[__$command_id]		           --- R.[__$command_id]
				   ,[ErrorStatus]		           --- R.[ErrorStatus]
				   ,[ErrorNumber]		           --- R.[ErrorNumber]
				   ,[ProcessedTime]		           --- R.[ProcessedTime]
				   ,[TranceDate]		           --- R.[TranceDate]
				   ,[CycleNumber]		           --- R.[CycleNumber]
				   ,[LsnTime]		           --- R.[LsnTime]
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
				   ,SRC.[ProdDate]
				   ,SRC.[PeriodTypeId]
				   ,SRC.[ReportId]
				   ,SRC.[ItemId]
				   ,SRC.[CauseChanged]
				   ,SRC.[DateCreate]
				   ,SRC.[DateUpdate]
				   ,SRC.[HistoryUserId]
				   ,SRC.[__$command_id]
				   ,SRC.[ErrorStatus]
				   ,SRC.[ErrorNumber]
				   ,SRC.[ProcessedTime]
				   ,SRC.[TranceDate]
				   ,@CycleCount -- SRC.[CycleNumber]
				   ,SRC.[LsnTime]
			  );

		SET @RowCount=@@ROWCOUNT
--		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAllPT_Free+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [dwh].[asto_Rows_CT_PT_free]

---=================================================================================
-- Новый цикл инкрементальной вставки в [arсh].[asto_Rows_CT]
	SET @RowCount=1
	SET @Offset = 0
	SET @CycleCount = 1
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize в [arсh].[asto_Rows_CT]
	MERGE [arсh].[asto_Rows_CT] WITH (HOLDLOCK) AS TGT
	USING 
		   (SELECT 
			R.[__$start_lsn]
           ,R.[__$end_lsn]
           ,R.[__$seqval]
           ,R.[__$operation]
           ,R.[__$update_mask]
           ,R.[Id]
           ,R.[ClosePeriodId]
           --,R.[ProdDate]
           --,R.[PeriodTypeId]
           --,R.[ReportId]
           ,R.[ItemId]
           ,R.[CauseChanged]
           ,R.[DateCreate]
           ,R.[DateUpdate]
           ,R.[HistoryUserId]
           ,R.[__$command_id]
           ,R.[ErrorStatus]
           ,R.[ErrorNumber]
           ,R.[ProcessedTime]
           ,R.[TranceDate]
           ,R.[CycleNumber]
           ,R.[LsnTime]
		  FROM [stg].[asto_Rows_CT] R 
		  ORDER BY R.[RowId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY		
			) SRC
		ON TGT.[__$start_lsn] = SRC.[__$start_lsn]
			AND  TGT.[__$seqval] = SRC.[__$seqval]
			AND  TGT.[__$operation] = SRC.[__$operation]
	WHEN NOT MATCHED THEN
		INSERT (
					[__$start_lsn]		           --- R.[__$start_lsn]
				   ,[__$end_lsn]		           --- R.[__$end_lsn]
				   ,[__$seqval]		           --- R.[__$seqval]
				   ,[__$operation]		           --- R.[__$operation]
				   ,[__$update_mask]		           --- R.[__$update_mask]
				   ,[Id]		           --- R.[Id]
				   ,[ClosePeriodId]		           --- R.[ClosePeriodId]
				   --,[ProdDate]		           --- R.[ProdDate]
				   --,[PeriodTypeId]		           --- R.[PeriodTypeId]
				   --,[ReportId]		           --- R.[ReportId]
				   ,[ItemId]		           --- R.[ItemId]
				   ,[CauseChanged]		           --- R.[CauseChanged]
				   ,[DateCreate]		           --- R.[DateCreate]
				   ,[DateUpdate]		           --- R.[DateUpdate]
				   ,[HistoryUserId]		           --- R.[HistoryUserId]
				   ,[__$command_id]		           --- R.[__$command_id]
				   ,[ErrorStatus]		           --- R.[ErrorStatus]
				   ,[ErrorNumber]		           --- R.[ErrorNumber]
				   ,[ProcessedTime]		           --- R.[ProcessedTime]
				   ,[TranceDate]		           --- R.[TranceDate]
				   ,[CycleNumber]		           --- R.[CycleNumber]
				   ,[LsnTime]		           --- R.[LsnTime]
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
				   --,SRC.[ProdDate]
				   --,SRC.[PeriodTypeId]
				   --,SRC.[ReportId]
				   ,SRC.[ItemId]
				   ,SRC.[CauseChanged]
				   ,SRC.[DateCreate]
				   ,SRC.[DateUpdate]
				   ,SRC.[HistoryUserId]
				   ,SRC.[__$command_id]
				   ,SRC.[ErrorStatus]
				   ,SRC.[ErrorNumber]
				   ,SRC.[ProcessedTime]
				   ,SRC.[TranceDate]
				   ,@CycleCount -- SRC.[CycleNumber]
				   ,SRC.[LsnTime]
			  );

		SET @RowCount=@@ROWCOUNT
-- /*debug*/		print CONCAT('@RowCount =', CAST(@RowCount AS NVARCHAR), ' --- @CycleCount = ',  CAST(@CycleCount AS NVARCHAR), ' --- @Offset = ',  CAST(@Offset AS NVARCHAR))
		SET @RowCountAllArh+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- Конец цикла итераций по @BatchSize в [arсh].[asto_Rows_CT]

		SET @TimeEnd = SYSDATETIME();
		SET @TimeElapsed_s=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

		IF @RowCountAll>0 OR @RowCountAllPT_Free>0 OR @RowCountAllArh>0
			BEGIN
				SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_Rows_CT]. Записано %i строк в [dwh].[asto_Rows_CT]. Записано %i строк в [arсh].[asto_Rows_CT]. Затрачено времени - %i сек.',@RowCountAll, @RowCountAllArh, @TimeElapsed_s);
				EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
			END
		ELSE
			IF @DebugInfo=1
				BEGIN
					SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_Rows_CT]. Нет записей в [stg].[asto_Rows_CT]. Затрачено времени - %i сек.', @TimeElapsed_s);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Debug'
				END

---=======================================
-- Блок удаления данных из [stg] таблицы
	TRUNCATE TABLE [stg].[asto_Rows_CT]
---=======================================
-- /*debug*/	SELECT CONCAT(N'Вставлено - ', @RowCountAll, ' / в [dwh].[asto_Rows_CT]'), CONCAT(N'Вставлено - ', @RowCountAllPT_Free, ' / в [dwh].[asto_Rows_CT_PT_free]'), CONCAT(N'Вставлено - ', @RowCountAllArh, ' / в [arсh].[asto_Rows_CT]')

		END TRY
		BEGIN CATCH
			SELECT @Message=FORMATMESSAGE(N'[dwh].[usp_Load_DWH_asto_Rows_CT]. Ошибка истории CDC. Процедура : %s Ошибка %i : %s', ERROR_PROCEDURE(), ERROR_NUMBER(),ERROR_MESSAGE());
			EXEC [log].[usp_Add_Log_Event] @Message=@Message
		END CATCH
END;

---=========================================================
-- Testing

-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC
-- EXECUTE [CDC_DWH].[dwh].[usp_Load_DWH_asto_Rows_CT] @BatchSize = 10, @DebugInfo = 1  -- Сохранять в лог подробную информацию

 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_Rows_CT]
 --TRUNCATE TABLE [CDC_DWH].[dwh].[asto_Rows_CT_PT_free]
 --TRUNCATE TABLE [CDC_DWH].[arсh].[asto_Rows_CT]

 --SELECT * FROM [CDC_DWH].[dwh].[asto_Rows_CT]
 --SELECT * FROM [CDC_DWH].[dwh].[asto_Rows_CT_PT_free]
 --SELECT * FROM [CDC_DWH].[arсh].[asto_Rows_CT]
 --SELECT * FROM [CDC_DWH].[stg].[asto_Rows_CT]

-- SELECT [__$start_lsn], [__$seqval],[__$operation], COUNT (*) num FROM [stg].[asto_Rows_CT]
-- GROUP BY [__$start_lsn], [__$seqval],[__$operation]
-- ORDER BY Num desc