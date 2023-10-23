
USE [ASTO_ZF]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		<Алексеенко Виталий>
-- Create date: <2023-10-02>
-- Description:	<Отображение истрии значений по строке с учетом хранилища [CDC_DWH]>
-- 
-- =============================================
CREATE OR ALTER PROCEDURE [asto].[usp_GetHistoryDataValuesByConfigurationRowId]
--CREATE OR ALTER PROCEDURE [asto].[usp_GetHistoryDataValuesByConfigurationRowIdNew]
	@ReportId [int],	-- ИД отчёта
	@Date [date],		-- Месяц
	@RowId [int],		-- ИД строки из конфигуратора строк (НЕ !!! asto.Rows)
	@PeriodTypeId [smallint]=2 -- Месячный или какой нибудь другой период
AS
BEGIN
	SET NOCOUNT ON;

--DECLARE @RowId INT=144  
--DECLARE @Date DATE='20210202'

DECLARE @ClosePeriodId INT=[dic].[fn_Get_ClosePeriodId_By_Date] (@Date, 2, NUll, NULL);

SET @Date=EOMONTH(@Date) -- чтобы точно конец месяца был для CDC_DWH

SET @RowId = (	SELECT [R].[id]
				FROM [cfg].[fnt_Get_Configuration_Rows](@ReportId, @Date, @PeriodTypeId, 0) [C] LEFT JOIN [asto].[Rows] [R]
						ON [R].[ItemId] = [C].[ItemId]
					WHERE
						[C].[id] = @RowId AND
						[R].[ClosePeriodId] = @ClosePeriodId);

IF @RowId IS NULL
BEGIN
	DECLARE @ErrorMessage [nvarchar](512) = FORMAT(GETDATE(), N'yyyy-MM-dd hh:mm:ss') + N' | Information | [asto].[usp_GetHistoryDataValuesByRowId] | История по выбранной строке отсутствует';
	THROW 50010, @ErrorMessage, 1;
END;

DROP TABLE IF EXISTS #DataValues
CREATE TABLE #DataValues
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
--		,ClosePeriodId INT
		,[RowId] BIGINT
		,[DataTypeId] TINYINT
		,[ElementId] SMALLINT
		,[UnitId] SMALLINT
		,[Value] DECIMAL(19,10)
		,[DateCreate] DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[DataSourceCellId] TINYINT
		,[HistoryUserId] SMALLINT
	)

DROP TABLE IF EXISTS #DataValuesCDC_DWH
CREATE TABLE #DataValuesCDC_DWH
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
--		,ClosePeriodId INT
		,[RowId] BIGINT
		,[DataTypeId] TINYINT
		,[ElementId] SMALLINT
		,[UnitId] SMALLINT
		,[Value] DECIMAL(19,10)
		,[DateCreate] DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[DataSourceCellId] TINYINT
		,[HistoryUserId] SMALLINT
		,[CauseChanged] NVARCHAR(256)  -- такое поле есть в [DataValues] в [CDC_DWH]
		,[LsnTime] DATETIME
	)

DROP TABLE IF EXISTS #DataValuesUpdated
CREATE TABLE #DataValuesUpdated
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
--		,ClosePeriodId INT
		,[RowId] BIGINT
		,[DataTypeId] TINYINT
		,[ElementId] SMALLINT
		,[UnitId] SMALLINT
		,[Value] DECIMAL(19,10)
		,[DateCreate] DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[DataSourceCellId] TINYINT
		,[HistoryUserId] SMALLINT
	)

DROP TABLE IF EXISTS #DataValuesInserted
CREATE TABLE #DataValuesInserted
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
--		,ClosePeriodId INT
		,[RowId] BIGINT
		,[DataTypeId] TINYINT
		,[ElementId] SMALLINT
		,[UnitId] SMALLINT
		,[Value] DECIMAL(19,10)
		,[DateCreate] DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[DataSourceCellId] TINYINT
		,[HistoryUserId] SMALLINT
	)

DROP TABLE IF EXISTS #DataValuesDeleted
CREATE TABLE #DataValuesDeleted
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
--		,ClosePeriodId INT
		,[RowId] BIGINT
		,[DataTypeId] TINYINT
		,[ElementId] SMALLINT
		,[UnitId] SMALLINT
		,[Value] DECIMAL(19,10)
		,[DateCreate] DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[DataSourceCellId] TINYINT
		,[HistoryUserId] SMALLINT
	)

DROP TABLE IF EXISTS #Rows
CREATE TABLE #Rows
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
		,[ClosePeriodId] INT
		,[ItemId] BIGINT
		,[CauseChanged] NVARCHAR(256)
		,[DateCreate]  DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[HistoryUserId] SMALLINT
	)

DROP TABLE IF EXISTS #RowsCDC_DWH
CREATE TABLE #RowsCDC_DWH
	(
		__$start_lsn BINARY(10)
		,__$seqval BINARY(10)
		,__$operation INT
		,__$update_mask VARBINARY(12)
		,[Id] BIGINT
		,[ClosePeriodId] INT
		,[ItemId] BIGINT
		,[CauseChanged] NVARCHAR(256)
		,[DateCreate]  DATETIME2(3)
		,[DateUpdate] DATETIME2(3)
		,[HistoryUserId] SMALLINT
		,[LsnTime] DATETIME
	)

--DECLARE @RowId INT=148
--DECLARE @Date DATE='20210202'
	
DECLARE @from_Date DATETIME, @to_Date DATETIME

-- Берем первый день месяца у полученной дата
SELECT @from_Date =DATEADD(month, DATEDIFF(month, 0, @DATE), 0)
SET @to_Date = GETDATE()

--SELECT @from_Date

--SELECT @ClosePeriodId

IF @ClosePeriodId IS NULL
		BEGIN 
			Print 'ClosePeriodId is NULL'
			RETURN
		END

DECLARE @to_lsn binary(10), 
		@from_lsn binary(10), -- с какого LSN берем данные (с начала месяца или мин который есть для этой таблицы)
		@from_lsnDV binary(10), -- LSN с которого берем данные для таблицы DataValues
		@from_lsnR binary(10), -- LSN с которого берем данные для таблицы Raws
		@min_lsnDV binary(10), -- min LSN для табл. DataValues
		@min_lsnR binary(10); -- min LSN для табл. Raws

SET @from_lsn = sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', @from_Date);

SET @min_lsnDV=sys.fn_cdc_get_min_lsn('asto_DataValues')
SET @min_lsnR=sys.fn_cdc_get_min_lsn('asto_Rows')

SET @to_lsn = sys.fn_cdc_map_time_to_lsn('largest less than or equal', @to_Date);-- Конечный LSN текущая дата

SELECT @from_lsnDV=MAX (MyLsn) FROM (VALUES(@from_lsn),(@min_lsnDV)) T(MyLsn)
SELECT @from_lsnR=MAX (MyLsn) FROM (VALUES(@from_lsn),(@min_lsnR)) T(MyLsn)

INSERT INTO #DataValues
	(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId])
	SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId]
	FROM
	cdc.[fn_cdc_get_all_changes_asto_DataValues](@from_lsnDV, @to_lsn, N'all update old')
	WHERE RowId=@RowId

INSERT INTO #DataValuesCDC_DWH(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId],[CauseChanged],[LsnTime])
SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId],[CauseChanged],[LsnTime]
FROM [CDC_DWH].[dwh].[asto_DataValues_CT]
WHERE [ProdDate]=@Date
	AND [RowId]=@RowId

INSERT INTO #RowsCDC_DWH( __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[ClosePeriodId],[ItemId],[CauseChanged],[DateCreate],[DateUpdate],[HistoryUserId],[LsnTime])
SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[ClosePeriodId],[ItemId],[CauseChanged],[DateCreate],[DateUpdate],[HistoryUserId],[LsnTime]
FROM [CDC_DWH].[dwh].[asto_Rows_CT]
WHERE [ProdDate]=@Date
	AND [Id]=@RowId

INSERT INTO #Rows
		(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[ClosePeriodId],[ItemId],[CauseChanged],[DateCreate],[DateUpdate],[HistoryUserId])
SELECT 
	__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[ClosePeriodId],[ItemId],[CauseChanged],[DateCreate],[DateUpdate],[HistoryUserId]
FROM [cdc].[fn_cdc_get_all_changes_asto_Rows](@from_lsnR, @to_lsn, N'all update old') 
WHERE id=@RowId

INSERT INTO #DataValuesUpdated
	(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId])
	SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId]
	FROM #DataValues
	WHERE __$operation=3 OR __$operation=4

INSERT INTO #DataValuesInserted
(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId])
	SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId]FROM #DataValues
	WHERE __$operation=2

INSERT INTO #DataValuesDeleted
(__$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId])
	SELECT __$start_lsn,__$seqval,__$operation,__$update_mask,[Id],[RowId],[DataTypeId],[ElementId],[UnitId],[Value],[DateCreate],[DateUpdate],[DataSourceCellId],[HistoryUserId]
	FROM #DataValues
	WHERE __$operation=1

--=================================================================================
;WITH DV_CTE
AS
(
	SELECT
		 Updated4.RowId
		,Before3.ElementId ElementId
		,Updated4.DataTypeId as DataTypeId
		,Before3.UnitId
		,Before3.Value as OldValue
		,Updated4.Value as NewValue
		,sys.fn_cdc_map_lsn_to_time(Updated4.__$start_lsn) as DateUpdate
		,Updated4.__$operation
		,Updated4.__$seqval
		,Updated4.__$start_lsn
		,Updated4.DataSourceCellId
	FROM #DataValuesUpdated Before3
		INNER JOIN #DataValuesUpdated Updated4
			ON	Before3.DataTypeId=Updated4.DataTypeId
				AND Before3.ElementId=Updated4.ElementId
				AND Before3.__$operation=3
				AND Updated4.__$operation=4
				AND Before3.__$seqval=Updated4.__$seqval
	UNION ALL
	SELECT
		 Inserted2.RowId
		,Inserted2.ElementId ElementId
		,Inserted2.DataTypeId as DataTypeId
		,Inserted2.UnitId
		,NULL as OldValue
		,Inserted2.Value as NewValue
		, sys.fn_cdc_map_lsn_to_time(Inserted2.__$start_lsn) as DateUpdate
		,Inserted2.__$operation
		,Inserted2.__$seqval
		,Inserted2.__$start_lsn
		,Inserted2.DataSourceCellId
	FROM #DataValuesInserted Inserted2
	UNION ALL
	SELECT
		 Deleted1.RowId
		,Deleted1.ElementId ElementId
		,Deleted1.DataTypeId as DataTypeId
		,Deleted1.UnitId
		,Deleted1.Value as OldValue
		,NULL as NewValue
		,sys.fn_cdc_map_lsn_to_time(Deleted1.__$start_lsn) as DateUpdate 
		,Deleted1.__$operation
		,Deleted1.__$seqval
		,Deleted1.__$start_lsn
		,Deleted1.DataSourceCellId
	FROM #DataValuesDeleted Deleted1
)
,
DV_ROWs_CTE
AS
(
SELECT 
	 HistTbl.[RowId]
	,HistTbl.[DataTypeId] as [DataTypeId]
	,HistTbl.UnitId
	,HistTbl.[ElementId] [ElementId]
	,HistTbl.[OldValue] [OldValue]
	,HistTbl.[NewValue] [NewValue]
	,HistTbl.[DateUpdate] [DateUpdate]
	,R.[CauseChanged] [CauseChanged]
	,R.[HistoryUserId] [HistoryUserId]
	,HistTbl.[__$operation]
	,HistTbl.[__$seqval]
	,HistTbl.[__$start_lsn]
	,HistTbl.DataSourceCellId
FROM DV_CTE [HistTbl]
INNER JOIN #Rows R
	ON R.[__$start_lsn]=HistTbl.[__$start_lsn] AND (R.[__$operation]=4 OR R.[__$operation]=2)
)
,
DV_DWH
AS
(
SELECT
	 CDC_DWH_Updated4.RowId
	,CDC_DWH_Before3.ElementId ElementId -- 1
	,CDC_DWH_Updated4.DataTypeId -- 1 
	,CDC_DWH_Updated4.UnitId -- 1
	,CDC_DWH_Before3.[Value] as OldValue -- 1
	,CDC_DWH_Updated4.[Value] as NewValue -- 1
	,CDC_DWH_Updated4.[LsnTime] as DateUpdate -- 1
	,CDC_DWH_Updated4.[CauseChanged]   /*N'ПОКА нету причины'*/ as CauseChanged -- 1
	,CDC_DWH_Updated4.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId -- 1
	,CDC_DWH_Updated4.__$operation
	,CDC_DWH_Updated4.__$seqval
	,CDC_DWH_Updated4.__$start_lsn
	,CDC_DWH_Updated4.DataSourceCellId
FROM #DataValuesCDC_DWH CDC_DWH_Before3
	INNER JOIN #DataValuesCDC_DWH CDC_DWH_Updated4
		ON	CDC_DWH_Before3.DataTypeId=CDC_DWH_Updated4.DataTypeId
			AND CDC_DWH_Before3.ElementId=CDC_DWH_Updated4.ElementId
			AND CDC_DWH_Before3.__$operation=3
			AND CDC_DWH_Updated4.__$operation=4
			AND CDC_DWH_Before3.__$seqval=CDC_DWH_Updated4.__$seqval
UNION ALL
SELECT
	 CDC_DWH_Inserted2.RowId
	,CDC_DWH_Inserted2.ElementId ElementId
	,CDC_DWH_Inserted2.DataTypeId as DataTypeId
	,CDC_DWH_Inserted2.UnitId
	,NULL as OldValue
	,CDC_DWH_Inserted2.Value as NewValue
	,CDC_DWH_Inserted2.[LsnTime]  as DateUpdate
	,CDC_DWH_Inserted2.[CauseChanged] /*N'ПОКА нету причины'*/ as CauseChanged
	,CDC_DWH_Inserted2.[HistoryUserId]  /*N'ПОКА нету UserId'*/ as HistoryUserId
	,CDC_DWH_Inserted2.__$operation
	,CDC_DWH_Inserted2.__$seqval
	,CDC_DWH_Inserted2.__$start_lsn
	,CDC_DWH_Inserted2.DataSourceCellId
FROM #DataValuesCDC_DWH	CDC_DWH_Inserted2
WHERE CDC_DWH_Inserted2.[__$operation]=2
UNION ALL
SELECT
	 CDC_DWH_Deleted1.RowId
	,CDC_DWH_Deleted1.ElementId as ElementId
	,CDC_DWH_Deleted1.DataTypeId as DataTypeId
	,CDC_DWH_Deleted1.UnitId
	,CDC_DWH_Deleted1.Value as OldValue
	,NULL as NewValue
	,CDC_DWH_Deleted1.[LsnTime] as [DateUpdate] 
	,CDC_DWH_Deleted1.[CauseChanged] /*N'ПОКА нету причины'*/ as CauseChanged
	,CDC_DWH_Deleted1.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId
	,CDC_DWH_Deleted1.__$operation
	,CDC_DWH_Deleted1.__$seqval
	,CDC_DWH_Deleted1.__$start_lsn
	,CDC_DWH_Deleted1.DataSourceCellId
FROM #DataValuesCDC_DWH	CDC_DWH_Deleted1
WHERE CDC_DWH_Deleted1.[__$operation]=1
)
,
DV_ROWS_DWH
AS
(
SELECT 
	 DV.RowId
	,DV.ElementId ElementId -- 1
	,DV.DataTypeId -- 1 
	,DV.UnitId -- 1
	,DV.OldValue -- 1
	,DV.NewValue -- 1
	,DV.DateUpdate -- 1
	,DV.[CauseChanged]   /*N'ПОКА нету причины'*/ as CauseChanged -- 1
	,DV.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId -- 1
	,DV.__$operation
	,DV.__$seqval
	,DV.__$start_lsn
	,DV.DataSourceCellId
	,R.[CauseChanged] AS [RowCauseChanged]
	,R.[HistoryUserId] AS [RowHistoryUserId]
	,R.[LsnTime] AS [RowLsnTime]
FROM DV_DWH DV
INNER JOIN 	#RowsCDC_DWH R ON R.__$start_lsn=DV.__$start_lsn AND (R.__$operation=4 OR R.__$operation=2)
)
,
DV_All
AS
(
SELECT
	 DV_ROWs_CTE.[RowId]
	,DV_ROWs_CTE.[ElementId]
	,DV_ROWs_CTE.[DataTypeId]
	,DV_ROWs_CTE.[UnitId]
	,DV_ROWs_CTE.[OldValue] 
	,DV_ROWs_CTE.[NewValue] 
	,DV_ROWs_CTE.[DateUpdate] 
	,DV_ROWs_CTE.[CauseChanged]
	,DV_ROWs_CTE.[HistoryUserId]
	,DV_ROWs_CTE.[__$operation]
	,DV_ROWs_CTE.[__$seqval]
	,DV_ROWs_CTE.[__$start_lsn]
	,DV_ROWs_CTE.DataSourceCellId
FROM DV_ROWs_CTE
UNION ALL
SELECT
	 DV_ROWS_DWH.RowId
	,DV_ROWS_DWH.ElementId
	,DV_ROWS_DWH.DataTypeId
	,DV_ROWS_DWH.[UnitId]
	,DV_ROWS_DWH.OldValue
	,DV_ROWS_DWH.NewValue
	,DV_ROWS_DWH.DateUpdate
	--,CASE DV_ROWS_DWH.__$operation
	--	WHEN 1 THEN DV_ROWS_DWH.[RowCauseChanged]
	--	ELSE DV_ROWS_DWH.[CauseChanged]
	-- END AS [CauseChanged]
	,DV_ROWS_DWH.[RowCauseChanged]
	,CASE DV_ROWS_DWH.__$operation
		WHEN 1 THEN DV_ROWS_DWH.[RowHistoryUserId]
		ELSE DV_ROWS_DWH.[HistoryUserId]
	 END AS [HistoryUserId]
	,DV_ROWS_DWH.__$operation
	,DV_ROWS_DWH.__$seqval
	,DV_ROWS_DWH.__$start_lsn
	,DV_ROWS_DWH.DataSourceCellId
FROM DV_ROWS_DWH
)
SELECT 
	 ROW_NUMBER() OVER (ORDER BY DV.__$seqval DESC) AS LineId
	,DV.RowId
	,DV.ElementId
	,E.[Name] AS ElementName
	,DV.DataTypeId
	,DT.[DataTypeDescription] AS DataTypeName
	,DV.[UnitId]
	,U.[Name] AS UnitName
	,DV.OldValue
	,DV.NewValue
	,DV.DateUpdate
	,DV.[CauseChanged]   /*N'ПОКА нету причины'*/ as CauseChanged
	,DV.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId
	,HU.[UserName] AS UserName
	,DV.__$operation
	,DV.__$seqval
	,DV.__$start_lsn
	,DV.DataSourceCellId
	,DS.[Name] AS DataSourceName
FROM DV_All DV
	INNER JOIN [dic].[Elements] E ON DV.[ElementId]=E.[Id]
		INNER JOIN [dic].[DataTypes] DT ON DV.[DataTypeId]=DT.[Id]
			INNER JOIN [dic].[Units] U ON DV.[UnitId]=U.[Id]
				INNER JOIN [sec].[HistoryUsers] HU ON DV.[HistoryUserId]=HU.[Id]
					INNER JOIN [dic].[DataSources] DS ON DV.[DataSourceCellId]=DS.[Id]
ORDER BY 
--				__$start_lsn desc
				__$seqval desc

--SELECT
--	ROW_NUMBER() OVER (ORDER BY HistTbl.__$seqval DESC) as LineId,
--	 HistTbl.RowId
	
--	,HistTbl.DataTypeId as DataTypeId
--	,(SELECT TOP 1 [DataTypeDescription] from [dic].DataTypes Where id=HistTbl.DataTypeId) as DataTypeName
	
	
--	,HistTbl.ElementId

----	,HistTbl.ElementName
--	,(SELECT TOP 1 [Name] from [dic].[Elements] Where id=HistTbl.ElementId) ElementName

--	,HistTbl.OldValue
--	,HistTbl.NewValue
--	,HistTbl.DateUpdate
--	,R.CauseChanged
--	,R.HistoryUserId

----	,UserName
--	,(SELECT TOP 1 [UserName] from sec.HistoryUsers Where id=R.HistoryUserId) as UserName

--	,HistTbl.__$operation
--	,HistTbl.__$seqval
--	,HistTbl.__$start_lsn

--FROM
--(SELECT
-- Updated4.RowId
--,Before3.ElementId ElementId
--,Updated4.DataTypeId as DataTypeId
--,'ElementName' as ElementName
--,Before3.Value as OldValue
--,Updated4.Value as NewValue
--,sys.fn_cdc_map_lsn_to_time(Updated4.__$start_lsn) as DateUpdate
--,N'ПОКА нету причины' as CauseChanged
--,N'ПОКА нету UserId' as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,Updated4.__$operation
--,Updated4.__$seqval
--,Updated4.__$start_lsn
--FROM #DataValuesUpdated Before3
--	INNER JOIN #DataValuesUpdated Updated4
--		ON	Before3.DataTypeId=Updated4.DataTypeId
--			AND Before3.ElementId=Updated4.ElementId
--			AND Before3.__$operation=3
--			AND Updated4.__$operation=4
--			AND Before3.__$seqval=Updated4.__$seqval
--UNION ALL
--SELECT
-- Inserted2.RowId
--,Inserted2.ElementId ElementId
--,Inserted2.DataTypeId as DataTypeId
--,'ElementName' as ElementName
--,NULL as OldValue
--,Inserted2.Value as NewValue
--, sys.fn_cdc_map_lsn_to_time(Inserted2.__$start_lsn) as DateUpdate
--,N'ПОКА нету причины' as CauseChanged
--,N'ПОКА нету UserId' as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,Inserted2.__$operation
--,Inserted2.__$seqval
--,Inserted2.__$start_lsn
--FROM #DataValuesInserted Inserted2
--UNION ALL
--SELECT
-- Deleted1.RowId
--,Deleted1.ElementId ElementId
--,Deleted1.DataTypeId as DataTypeId
--, 'ElementName' as ElementName
--,Deleted1.Value as OldValue
--,NULL as NewValue
--,sys.fn_cdc_map_lsn_to_time(Deleted1.__$start_lsn) as DateUpdate 
--,N'ПОКА нету причины' as CauseChanged
--,N'ПОКА нету UserId' as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,Deleted1.__$operation
--,Deleted1.__$seqval
--,Deleted1.__$start_lsn
--FROM #DataValuesDeleted Deleted1

--UNION ALL
--SELECT
-- CDC_DWH_Updated4.RowId
--,CDC_DWH_Before3.ElementId ElementId
--,CDC_DWH_Updated4.DataTypeId as DataTypeId
--,'ElementName' as ElementName
--,CDC_DWH_Before3.Value as OldValue
--,CDC_DWH_Updated4.Value as NewValue
--,CDC_DWH_Updated4.[LsnTime] as DateUpdate
--,CDC_DWH_Updated4.[CauseChanged]   /*N'ПОКА нету причины'*/ as CauseChanged
--,CDC_DWH_Updated4.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,CDC_DWH_Updated4.__$operation
--,CDC_DWH_Updated4.__$seqval
--,CDC_DWH_Updated4.__$start_lsn
--FROM #DataValuesCDC_DWH CDC_DWH_Before3
--	INNER JOIN #DataValuesCDC_DWH CDC_DWH_Updated4
--		ON	CDC_DWH_Before3.DataTypeId=CDC_DWH_Updated4.DataTypeId
--			AND CDC_DWH_Before3.ElementId=CDC_DWH_Updated4.ElementId
--			AND CDC_DWH_Before3.__$operation=3
--			AND CDC_DWH_Updated4.__$operation=4
--			AND CDC_DWH_Before3.__$seqval=CDC_DWH_Updated4.__$seqval
--UNION ALL
--SELECT
-- CDC_DWH_Inserted2.RowId
--,CDC_DWH_Inserted2.ElementId ElementId
--,CDC_DWH_Inserted2.DataTypeId as DataTypeId
--,'ElementName' as ElementName
--,NULL as OldValue
--,CDC_DWH_Inserted2.Value as NewValue
--,CDC_DWH_Inserted2.[LsnTime]  as DateUpdate
--,CDC_DWH_Inserted2.[CauseChanged] /*N'ПОКА нету причины'*/ as CauseChanged
--,CDC_DWH_Inserted2.[HistoryUserId]  /*N'ПОКА нету UserId'*/ as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,CDC_DWH_Inserted2.__$operation
--,CDC_DWH_Inserted2.__$seqval
--,CDC_DWH_Inserted2.__$start_lsn
--FROM #DataValuesCDC_DWH	CDC_DWH_Inserted2
--WHERE CDC_DWH_Inserted2.[__$operation]=2
--UNION ALL
--SELECT
-- CDC_DWH_Deleted1.RowId
--,CDC_DWH_Deleted1.ElementId as ElementId
--,CDC_DWH_Deleted1.DataTypeId as DataTypeId
--,'ElementName' as ElementName
--,CDC_DWH_Deleted1.Value as OldValue
--,NULL as NewValue
--,CDC_DWH_Deleted1.[LsnTime] as DateUpdate 
--,CDC_DWH_Deleted1.[CauseChanged] /*N'ПОКА нету причины'*/ as CauseChanged
--,CDC_DWH_Deleted1.[HistoryUserId] /*N'ПОКА нету UserId'*/ as HistoryUserId
--,N'ПОКА нету UserName' as UserName
--,CDC_DWH_Deleted1.__$operation
--,CDC_DWH_Deleted1.__$seqval
--,CDC_DWH_Deleted1.__$start_lsn
--FROM #DataValuesCDC_DWH	CDC_DWH_Deleted1
--WHERE CDC_DWH_Deleted1.[__$operation]=1
--) HistTbl
--INNER JOIN #Rows R
--	ON R.__$start_lsn=HistTbl.__$start_lsn AND (R.__$operation=4 OR R.__$operation=2)
--ORDER BY 
----				__$start_lsn desc
--				__$seqval desc

DROP TABLE #DataValues
DROP TABLE #DataValuesCDC_DWH
DROP TABLE #Rows
DROP TABLE #DataValuesUpdated
DROP TABLE #DataValuesInserted
DROP TABLE #DataValuesDeleted

END

-- EXEC [asto].[usp_GetHistoryDataValuesByConfigurationRowId] 132, N'20210501', 9398 

--- 20230425 Свежий пример

-- EXEC [asto].[usp_GetHistoryDataValuesByConfigurationRowId] 56, N'20230401', 701876, 2 

-- Проблемы с DateUpdate
-- EXEC [asto].[usp_GetHistoryDataValuesByConfigurationRowId] 204, N'20230401', 642657, 2 