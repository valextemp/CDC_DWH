-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230330
-- Description:	Загрузка данных из asto_DataValues_CT в [stg].[asto_DataValues_CT]
-- =============================================
-- Comments
-- [IsValueChanged] буду обрабатывать в [stg] а пока по дефолту будет 1
--
-- =============================================

CREATE       PROCEDURE [stg].[usp_Load_asto_DataValues_CT]
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
--	DECLARE @BatchSize INT=50000
	DECLARE @RowCount INT =1
	DECLARE @RowCountAll INT = 0 -- счетчик сколько всего вставлено записей во всех итерациях
	DECLARE @MaxRows INT
	DECLARE @Offset INT = 0
	DECLARE @CycleCount INT =1


--SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
WHILE @RowCount>0
	BEGIN -- начала цикла
	;WITH DV_CTE
	AS
	(
		SELECT [__$start_lsn]
			  ,[__$end_lsn]
			  ,[__$seqval]
			  ,[__$operation]
			  ,[__$update_mask]
			  ,[Id]
			  --,[ClosePeriodId] -- больше нет
			  ,[RowId]
			  ,[ElementId]
			  ,[DataTypeId]
			  ,[UnitId]
			  ,[Value]
			  ,[DateCreate]
			  ,[DateUpdate]
			  ,[ShipConsignmentId]
			  ,[DataSourceCellId]
			  ,[HistoryUserId]  -- новое поле 20230420
			  ,[__$command_id]
			  ,NULL as [LsnTime] -- пока так сделаю, буду в etl процессе вставлять
			  --,[ASTO_ZF].sys.fn_cdc_map_lsn_to_time([__$start_lsn]) as [LsnTime]
		  FROM [ASTO_ZF].[cdc].[asto_DataValues_CT]
		  ORDER BY [__$start_lsn], [__$seqval],[__$operation]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY

	)
		MERGE [stg].[asto_DataValues_CT] WITH(HOLDLOCK) AS TARGET
		USING 
		(
		SELECT [__$start_lsn]
			  ,[__$end_lsn]
			  ,[__$seqval]
			  ,[__$operation]
			  ,[__$update_mask]
			  ,[Id]
			  --,[ClosePeriodId] -- больше нет
			  ,[RowId]
			  ,[ElementId]
			  ,[DataTypeId]
			  ,[UnitId]
			  ,[Value]
			  ,[DateCreate]
			  ,[DateUpdate]
			  ,[ShipConsignmentId]
			  ,[DataSourceCellId]
			  ,[HistoryUserId]  -- новое поле 20230420
			  ,[__$command_id]
			  ,[LsnTime]
			FROM DV_CTE
		) AS SOURCE
		ON TARGET.[__$start_lsn] = SOURCE.[__$start_lsn] AND TARGET.[__$seqval]=SOURCE.[__$seqval] AND TARGET.[__$operation]=SOURCE.[__$operation]
--		WHEN NOT MATCHED AND (SOURCE.[CDL_TIMESTAMP] IS NOT NULL) THEN 
		WHEN NOT MATCHED THEN 
		INSERT 
		    ([__$start_lsn]
           ,[__$end_lsn]
           ,[__$seqval]
           ,[__$operation]
           ,[__$update_mask]
           ,[Id]
         --,[ClosePeriodId] -- больше нет
           ,[RowId]
           ,[ElementId]
           ,[DataTypeId]
           ,[UnitId]
           ,[Value]
           ,[DateCreate]
           ,[DateUpdate]
           ,[ShipConsignmentId]
           ,[DataSourceCellId]
		   ,[HistoryUserId]  -- новое поле 20230420
           ,[__$command_id]
           --,[ErrorStatus]
           --,[ErrorNumber]
           --,[ProcessedTime]
           ,[TranceDate]
           ,[CycleNumber]
           ,[LsnTime])
		VALUES(
		    SOURCE.[__$start_lsn]
           ,SOURCE.[__$end_lsn]
           ,SOURCE.[__$seqval]
           ,SOURCE.[__$operation]
           ,SOURCE.[__$update_mask]
           ,SOURCE.[Id]
           --,SOURCE.[ClosePeriodId] -- больше нет
           ,SOURCE.[RowId]
           ,SOURCE.[ElementId]
           ,SOURCE.[DataTypeId]
           ,SOURCE.[UnitId]
           ,SOURCE.[Value]
           ,SOURCE.[DateCreate]
           ,SOURCE.[DateUpdate]
           ,SOURCE.[ShipConsignmentId]
           ,SOURCE.[DataSourceCellId]
		   ,SOURCE.[HistoryUserId]  -- новое поле 20230420
           ,SOURCE.[__$command_id]
           --,SOURCE.[ErrorStatus]
           --,SOURCE.[ErrorNumber]
           --,SOURCE.[ProcessedTime]
           ,SYSDATETIME() -- SOURCE.[TranceDate]
           ,@CycleCount -- SOURCE.[CycleNumber]
           ,SOURCE.[LsnTime]);

		SET @RowCount=@@ROWCOUNT
		SET @RowCountAll+=@RowCount
		SET @CycleCount+=1
		SET @Offset+=@BatchSize
		IF @RowCount<@BatchSize
			BREAK
	END -- end цикла

	--!! Добавить еще удаление из исторических таблиц в ASTO_ZF

		SET @TimeEnd = SYSDATETIME();
		SET @TimeElapsed_s=ISNULL(DATEDIFF(second,@TimeStart,@TimeEnd),0);

		IF @RowCountAll>0
			BEGIN
				TRUNCATE TABLE [ASTO_ZF].[cdc].[asto_DataValues_CT]
				SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Load_asto_DataValues_CT]. Вставлено %i записей в [stg].[asto_DataValues_CT]. Затрачено времени - %i мсек.',@RowCountAll, @TimeElapsed_s);
				EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
			END
		ELSE
			IF @DebugInfo=1
				BEGIN
					SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Load_asto_DataValues_CT]. Нет записей для вставки в [stg].[asto_DataValues_CT] . Затрачено времени - %i мсек.', @TimeElapsed_s);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Debug'
				END

END

-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC
--	TRUNCATE TABLE [CDC_DWH].[log].[Events] 

-- SELECT *  FROM [ASTO_ZF].[cdc].[asto_DataValues_CT] ORDER BY [__$start_lsn] DESC, [__$seqval] DESC,[__$operation] DESC
-- SELECT * FROM [CDC_DWH].[stg].[asto_DataValues_CT] ORDER BY [DvId]

--UPDATE [ASTO_ZF].[asto].[DataValues] -- эмуляция что изменилось полье не Value
--SET [DataSourceCellId]=9
--WHERE [Id]= 6155693

-- EXEC [CDC_DWH].[stg].[usp_Load_asto_DataValues_CT] @BatchSize=100

-- TRUNCATE TABLE [CDC_DWH].[stg].[asto_DataValues_CT]