CREATE TABLE [log].[Events] (
    [Id]        BIGINT         IDENTITY (1, 1) NOT NULL,
    [TimeEvent] DATETIME       NOT NULL,
    [Level]     NVARCHAR (32)  NOT NULL,
    [Source]    NVARCHAR (100) NOT NULL,
    [UserName]  NVARCHAR (100) NULL,
    [Category]  NVARCHAR (32)  NOT NULL,
    [Message]   NVARCHAR (800) NOT NULL,
    [Details]   XML            NULL,
    PRIMARY KEY CLUSTERED ([Id] ASC) ON [LOGGING_FG]
) ON [LOGGING_FG] TEXTIMAGE_ON [LOGGING_FG];

