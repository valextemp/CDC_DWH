﻿CREATE TABLE [arсh].[asto_Rows_CT] (
    [RowId]          INT             IDENTITY (1, 1) NOT NULL,
    [__$start_lsn]   BINARY (10)     NOT NULL,
    [__$end_lsn]     BINARY (10)     NULL,
    [__$seqval]      BINARY (10)     NOT NULL,
    [__$operation]   INT             NOT NULL,
    [__$update_mask] VARBINARY (128) NULL,
    [Id]             BIGINT          NULL,
    [ClosePeriodId]  INT             NULL,
    [ItemId]         INT             NULL,
    [CauseChanged]   NVARCHAR (256)  NULL,
    [DateCreate]     DATETIME2 (3)   NULL,
    [DateUpdate]     DATETIME2 (3)   NULL,
    [HistoryUserId]  SMALLINT        NULL,
    [__$command_id]  INT             NULL,
    [ErrorStatus]    TINYINT         DEFAULT ((0)) NOT NULL,
    [ErrorNumber]    SMALLINT        DEFAULT ((0)) NOT NULL,
    [ProcessedTime]  DATETIME2 (3)   NULL,
    [TranceDate]     DATETIME2 (3)   DEFAULT (sysdatetime()) NULL,
    [CycleNumber]    INT             DEFAULT ((1)) NOT NULL,
    [LsnTime]        DATETIME        NOT NULL,
    PRIMARY KEY CLUSTERED ([RowId] ASC) ON [ARCHIVE_FG]
) ON [ARCHIVE_FG];

