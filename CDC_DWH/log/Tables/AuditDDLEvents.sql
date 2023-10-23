CREATE TABLE [log].[AuditDDLEvents] (
    [Id]               INT             IDENTITY (1, 1) NOT NULL,
    [PostTime]         DATETIME        NOT NULL,
    [EventType]        NVARCHAR (128)  NOT NULL,
    [LoginName]        NVARCHAR (128)  NOT NULL,
    [SchemaName]       NVARCHAR (128)  NULL,
    [ObjectName]       NVARCHAR (128)  NOT NULL,
    [TargetObjectName] NVARCHAR (128)  NULL,
    [CommandText]      NVARCHAR (4000) NULL,
    [StarUMLCorrect]   BIT             NOT NULL,
    [Comment]          NVARCHAR (256)  NULL,
    [EventData]        XML             NOT NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC) ON [LOGGING_FG]
) ON [LOGGING_FG] TEXTIMAGE_ON [LOGGING_FG];

