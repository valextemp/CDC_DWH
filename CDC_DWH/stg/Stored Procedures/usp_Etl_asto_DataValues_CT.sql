-- =============================================
-- Author:		Alekseenko Vitaliy
-- Create date: 20230330
-- Description:	Обработка данных из [stg].[asto_DataValues_CT] и дальнейщая передача в архиы и хранилище
-- =============================================

CREATE   PROCEDURE [stg].[usp_Etl_asto_DataValues_CT]
	@BatchSize INT = 300000, -- Количество записей за цикл (инкрементальная загрузка)
	@DebugInfo INT=1  -- Сохранять в лог подробную информацию
AS
BEGIN
	SET NOCOUNT ON;
	-- Важный параметр, важный параметр бит для поля [value] в битовой маске [__$update_mask]
	--DECLARE @ValueBit1 VARBINARY(128) = CAST(POWER(2,[ASTO_ZF].[sys].[fn_cdc_get_column_ordinal] ('asto_DataValues','Value')-1) AS VARBINARY(128))
	--SELECT @ValueBit1 --debug
	DECLARE @ValueBit BIGINT = CAST(POWER(2,[ASTO_ZF].[sys].[fn_cdc_get_column_ordinal] ('asto_DataValues','Value')-1) AS BIGINT)
	--SELECT @ValueBit --debug

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

	DECLARE @ErrorNumber SMALLINT = 0

BEGIN TRY
WHILE @RowCount>0
	BEGIN -- Начала цикла итераций по @BatchSize

		SET @ProcessedTime=SYSDATETIME()

		;WITH R_CTE
		AS
		(
		SELECT [Id] as [RowId],[ClosePeriodId],[ItemId], COUNT(*) num FROM [stg].[asto_Rows_CT] GROUP BY [Id],[ClosePeriodId],[ItemId]
		),
		aR_CTE
		AS
		(
		SELECT [Id] as [RowId],[ClosePeriodId],[ItemId], COUNT(*) num FROM [ASTO_ZF].[asto].[Rows] 
		WHERE [ClosePeriodId] IN (SELECT [ClosePeriodId] FROM [stg].[asto_Rows_CT] GROUP BY [ClosePeriodId])
		GROUP BY [Id],[ClosePeriodId],[ItemId]
		),
		R_CTE_full
		AS
		(
			SELECT R_CTE.[RowId],R_CTE.[ClosePeriodId], R_CTE.[ItemId], 
			 	CP.[LastDay], CP.[PeriodTypeId], I.[ReportId]	
			FROM R_CTE
				INNER JOIN [ASTO_ZF].[asto].[ClosePeriods] CP ON R_CTE.[ClosePeriodId]=CP.[Id]
					INNER JOIN [ASTO_ZF].[asto].[Items] I ON I.[Id]=R_CTE.[ItemId]
		),
		aR_CTE_full
		AS
		(
			SELECT aR_CTE.[RowId],aR_CTE.[ClosePeriodId],aR_CTE.[ItemId], 
			 	CP.[LastDay], CP.[PeriodTypeId], I.[ReportId]	
			FROM aR_CTE
				INNER JOIN [ASTO_ZF].[asto].[ClosePeriods] CP ON aR_CTE.[ClosePeriodId]=CP.[Id]
					INNER JOIN [ASTO_ZF].[asto].[Items] I ON I.[Id]=aR_CTE.[ItemId]
		)
		,
		R_CH -- CTE для получения Причин изменения. Исходим из того что причины изм находятся в тойже выгрузке ячеек и строк из Раб. БД
			 -- Соответственно на момент ETL процесса данные по строкам должны быть выгружены на этот момент
		AS
		(
			SELECT [__$start_lsn],[__$operation],[Id] AS [RowId],[CauseChanged] FROM [stg].[asto_Rows_CT] WHERE [CauseChanged] IS NOT NULL

		)
		, DV_CTE
		 AS
		 (
			SELECT
			   DVal.[DvId]
			  ,DVal.[__$start_lsn]
			  ,DVal.[__$end_lsn]
			  ,DVal.[__$seqval]
			  ,DVal.[__$operation]
			  ,DVal.[__$update_mask]
			  ,DVal.[Id]
			  ,DVal.[ClosePeriodId]
			  ,DVal.[RowId]
			  ,DVal.[ProdDate] --- Добавленные мной поля
			  ,DVal.[PeriodTypeId] --- Добавленные мной поля
			  ,DVal.[ReportId] --- Добавленные мной поля   
			  ,DVal.[ItemId]
			  ,DVal.[ElementId]
			  ,DVal.[DataTypeId]
			  ,DVal.[UnitId]
			  ,DVal.[Value]
			  ,DVal.[CauseChanged]
			  ,DVal.[DateCreate]
			  ,DVal.[DateUpdate]
			  ,DVal.[ShipConsignmentId]
			  ,DVal.[DataSourceCellId]
			  ,DVal.[__$command_id]	
			  ,DVal.[IsValueChanged]
			  ,DVal.[ErrorStatus]
			  ,DVal.[ErrorNumber]
			  ,DVal.[ProcessedTime]
			  ,DVal.[TranceDate]
			  ,DVal.[CycleNumber]
			  ,DVal.[LsnTime]

		  FROM [stg].[asto_DataValues_CT] DVal	 
		  --WHERE [ErrorStatus] = 0 -- 0 значит не обработанные строки, но буду его обновлять в процессе
		  ORDER BY DVal.[DvId]
		  OFFSET @Offset ROWS FETCH FIRST @BatchSize ROWS ONLY	
		) 
		UPDATE  DV
			SET 
				@ErrorNumber = ( -- Проверяем на ошибку и используем ниже 
						0 |
						IIF([ASTO_ZF].sys.fn_cdc_map_lsn_to_time(DV.[__$start_lsn]) IS NULL, POWER(2,1-1), 0) | -- [LsnTime] Не должен быть NULL
						IIF(COALESCE(Rf.[LastDay],aRf.[LastDay],'20000131') =  '20000131', POWER(2,2-1), 0) | -- [ProdDate] не нашелся 
						IIF(COALESCE(Rf.[PeriodTypeId],aRf.[PeriodTypeId],0) = 0 , POWER(2,3-1), 0) | -- [PeriodTypeId] аналогично если два поля выше 
						IIF(COALESCE(Rf.[ReportId],aRf.[ReportId],0) = 0, POWER(2,4-1), 0) | -- [ReportId] не шнашелся (не должно быть в принципе) 
						IIF(COALESCE(Rf.[ItemId],aRf.[ItemId],0) = 0, POWER(2,5-1), 0) | -- [ItemId] не шнашелся (не должно быть в принципе)
						IIF(COALESCE(Rf.[ClosePeriodId],aRf.[ClosePeriodId],0) =  0, POWER(2,6-1), 0) |  -- [ProdDate] не нашелся (Хотя из-за INNER JOIN 
						IIF(DV.__$update_mask & @ValueBit = 0  , POWER(2,7-1), 0)  -- [Value] не менялось, а менялось/менялися какие-то другие, считаем что это ошибка 
								)
			   ,DV.[ClosePeriodId] = COALESCE(Rf.[ClosePeriodId],aRf.[ClosePeriodId],0)
			   ,DV.[ProdDate] = COALESCE(Rf.[LastDay],aRf.[LastDay],'20000131') 
			   ,DV.[PeriodTypeId] = COALESCE(Rf.[PeriodTypeId],aRf.[PeriodTypeId],0) 
			   ,DV.[ReportId] = COALESCE(Rf.[ReportId],aRf.[ReportId],0)
			   ,DV.[ItemId]  = COALESCE(Rf.[ItemId],aRf.[ItemId],0)
			   ,DV.[CauseChanged] = R_CH.[CauseChanged]
			   ,DV.[IsValueChanged] = CASE WHEN DV.[__$update_mask] & @ValueBit = 0 THEN 0 ELSE 1 END
			   --IIF(DV.[__$update_mask] & @ValueBit > 0 , 1

			   ,DV.[ProcessedTime] = @ProcessedTime
			   ,DV.[LsnTime] = [ASTO_ZF].sys.fn_cdc_map_lsn_to_time(DV.[__$start_lsn])
			   ,DV.[CycleNumber] +=(@CycleCount+1000) --debug
			   --- [ErrorNumber] processing
			   ,DV.[ErrorStatus] = CASE	
										WHEN @ErrorNumber>0 THEN 2
										ELSE 1
								   END
						--	(
						--		IIF([ASTO_ZF].sys.fn_cdc_map_lsn_to_time(DV_CTE.[__$start_lsn]) IS NULL, POWER(2,1-1), 0) | -- [LsnTime] Не должен быть NULL
						--		IIF(COALESCE(Rf.[LastDay],aRf.[LastDay],'20000131') =  '20000131', POWER(2,2-1), 0) | -- [ProdDate] не нашелся (Хотя из-за INNER JOIN такого быть не может)
						--		IIF(COALESCE(Rf.[PeriodTypeId],aRf.[PeriodTypeId],0) = 0 , POWER(2,3-1), 0) | -- [PeriodTypeId] аналогично если два поля выше в ошибке (Хотя из-за INNER JOIN такого быть не может)
						--		IIF(COALESCE(Rf.[ReportId],aRf.[ReportId],0) = 0, POWER(2,4-1), 0) | -- [ReportId] не шнашелся (не должно быть в принципе) (Хотя из-за INNER JOIN такого быть не может)
						--		IIF(COALESCE(Rf.[ClosePeriodId],aRf.[ClosePeriodId],0) =  0, POWER(2,5-1), 0)  -- [ProdDate] не нашелся (Хотя из-за INNER JOIN 
						--	)>0  THEN 2 -- есть ошибка
						--ELSE 1 -- Ошибок нет
			   ,DV.[ErrorNumber] = @ErrorNumber
			 --   (
				--IIF([ASTO_ZF].sys.fn_cdc_map_lsn_to_time(DV_CTE.[__$start_lsn]) IS NULL, POWER(2,1-1), 0) | -- [LsnTime] Не должен быть NULL
				--IIF(COALESCE(Rf.[LastDay],aRf.[LastDay],'20000131') =  '20000131', POWER(2,2-1), 0) | -- [ProdDate] не нашелся (Хотя из-за INNER JOIN такого быть не может)
				--IIF(COALESCE(Rf.[PeriodTypeId],aRf.[PeriodTypeId],0) = 0 , POWER(2,3-1), 0) | -- [PeriodTypeId] аналогично если два поля выше в ошибке (Хотя из-за INNER JOIN такого быть не может)
				--IIF(COALESCE(Rf.[ReportId],aRf.[ReportId],0) = 0, POWER(2,4-1), 0) | -- [ReportId] не шнашелся (не должно быть в принципе) (Хотя из-за INNER JOIN такого быть не может)
				--IIF(COALESCE(Rf.[ClosePeriodId],aRf.[ClosePeriodId],0) =  0, POWER(2,5-1), 0)  -- [ProdDate] не нашелся (Хотя из-за INNER JOIN 
				--)
		FROM DV_CTE DV
				LEFT JOIN R_CTE_full Rf ON DV.[RowId]=Rf.[RowId]
					LEFT JOIN aR_CTE_full aRf ON DV.[RowId]=aRf.[RowId]
						LEFT JOIN R_CH ON DV.[RowId]=R_CH.[RowId] AND DV.[__$start_lsn]=R_CH.[__$start_lsn] AND R_CH.[__$operation] = 4 AND DV.[__$operation] IN (1,2,4) --Причины изменения подвязываем
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
				SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_DataValues_CT]. Обработано %i записей в [stg].[asto_DataValues_CT]. Затрачено времени - %i сек.',@RowCountAll, @TimeElapsed_s);
				EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Info'
			END
		ELSE
			IF @DebugInfo=1
				BEGIN
					SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_DataValues_CT]. Нет записей для обработки в [stg].[asto_DataValues_CT] . Затрачено времени - %i сек.', @TimeElapsed_s);
					EXEC [log].[usp_Add_Log_Event] @Message=@Message, @Level=N'Debug'
				END
		END TRY
		BEGIN CATCH
			SELECT @Message=FORMATMESSAGE(N'[stg].[usp_Etl_asto_DataValues_CT]. Ошибка истории CDC. Процедура : %s Ошибка %i : %s', ERROR_PROCEDURE(), ERROR_NUMBER(),ERROR_MESSAGE());
			EXEC [log].[usp_Add_Log_Event] @Message=@Message
		END CATCH

END;


-- SELECT * FROM [CDC_DWH].[log].[Events] ORDER BY [Id] DESC

-- EXECUTE [CDC_DWH].[stg].[usp_Etl_asto_DataValues_CT] @BatchSize = 300000, @DebugInfo = 1

-- TRUNCATE TABLE [CDC_DWH].[stg].[asto_DataValues_CT]

-- SELECT [CycleNumber], COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] GROUP BY [CycleNumber]

-- SELECT [ProdDate], COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] GROUP BY [ProdDate]

-- SELECT [ProcessedTime], COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] GROUP BY [ProcessedTime] ORDER BY [ProcessedTime]

--SELECT * FROM [CDC_DWH].[stg].[asto_DataValues_CT]

--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [ErrorNumber]>0
--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [ErrorNumber]=0

--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [PeriodTypeId]=0

--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [ReportId]=0

--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [ClosePeriodId]=0

--SELECT COUNT(*) FROM [CDC_DWH].[stg].[asto_DataValues_CT] WHERE [ProdDate]='19000131' OR [ProdDate]='20000131'
	

--SELECT * FROM [CDC_DWH].[stg].[asto_Rows_CT] WHERE [LsnTime] IS NULL

--UPDATE [CDC_DWH].[stg].[asto_DataValues_CT]
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