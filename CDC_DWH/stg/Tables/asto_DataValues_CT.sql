CREATE TABLE [stg].[asto_DataValues_CT] (
    [DvId]              INT              IDENTITY (1, 1) NOT NULL,
    [__$start_lsn]      BINARY (10)      NOT NULL,
    [__$end_lsn]        BINARY (10)      NULL,
    [__$seqval]         BINARY (10)      NOT NULL,
    [__$operation]      INT              NOT NULL,
    [__$update_mask]    VARBINARY (128)  NULL,
    [Id]                BIGINT           NULL,
    [ClosePeriodId]     INT              NULL,
    [ProdDate]          DATE             DEFAULT ('19000131') NOT NULL,
    [PeriodTypeId]      SMALLINT         DEFAULT ((0)) NOT NULL,
    [ReportId]          INT              DEFAULT ((0)) NOT NULL,
    [ItemId]            INT              DEFAULT ((0)) NOT NULL,
    [RowId]             BIGINT           NULL,
    [ElementId]         SMALLINT         NULL,
    [DataTypeId]        TINYINT          NULL,
    [UnitId]            SMALLINT         NULL,
    [Value]             DECIMAL (19, 10) NULL,
    [CauseChanged]      NVARCHAR (256)   NULL,
    [DateCreate]        DATETIME2 (3)    NULL,
    [DateUpdate]        DATETIME2 (3)    NULL,
    [ShipConsignmentId] INT              NULL,
    [DataSourceCellId]  TINYINT          NULL,
    [HistoryUserId]     SMALLINT         NULL,
    [__$command_id]     INT              NULL,
    [IsValueChanged]    BIT              DEFAULT ((1)) NOT NULL,
    [ErrorStatus]       TINYINT          DEFAULT ((0)) NOT NULL,
    [ErrorNumber]       SMALLINT         DEFAULT ((0)) NOT NULL,
    [ProcessedTime]     DATETIME2 (3)    NULL,
    [TranceDate]        DATETIME2 (3)    DEFAULT (sysdatetime()) NULL,
    [CycleNumber]       INT              DEFAULT ((1)) NOT NULL,
    [LsnTime]           DATETIME         NULL
) ON [DATA_FG];


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Таблица куда загружаются данные из АСТО, для последующей обработки', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Регистрационный номер транзакции в журнале (LSN), связанный с фиксацией транзакции изменения', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$start_lsn';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Этот столбец всегда имеет значение NULL', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$end_lsn';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Значение последовательности, используемое для упорядочивания изменений строк в пределах транзакции', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$seqval';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Операция DML, 1 - удаление, 2 - вставка, 3 - обновление (старые значения),4 - обновление (новые значения)', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$operation';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Битовая маска, показывает какие столбцы изменились', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$update_mask';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Отслеживает порядок операций в транзакции', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'__$command_id';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Показывает что было изменено поле [Value], а то бывает, что меняют другие поля, а [Value] не меняется', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'IsValueChanged';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Статус ошибки (если при обработке возникла ошибка 0-не обработанаб 1-обработано ошибок нет, 2-при обработке возникли ошибки)', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'ErrorStatus';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Номер ошибки', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'ErrorNumber';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Дата и время обработки', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'ProcessedTime';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Дата и время загрузки', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'TranceDate';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Цикл записи в который была сделана запись (будет инкрементальная загрузка напр по 500 000 запесей)', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'CycleNumber';


GO
EXECUTE sp_addextendedproperty @name = N'Description', @value = N'Для метки времени  [__$start_lsn] (Регистрационный номер транзакции в журнале (LSN), связанный с фиксацией транзакции изменения)', @level0type = N'SCHEMA', @level0name = N'stg', @level1type = N'TABLE', @level1name = N'asto_DataValues_CT', @level2type = N'COLUMN', @level2name = N'LsnTime';

