

CREATE TRIGGER [trg_audit_ddl_events]
ON DATABASE FOR DDL_DATABASE_LEVEL_EVENTS
AS
SET NOCOUNT ON;
DECLARE @eventdata AS XML = eventdata();
INSERT INTO log.AuditDDLEvents
(
	PostTime,
	EventType,
	LoginName,
	SchemaName,
	ObjectName,
	TargetObjectName,
	CommandText,
	StarUMLCorrect,
	Comment,
	[Eventdata] 
)
	VALUES(
	@eventdata.value('(/EVENT_INSTANCE/PostTime)[1]',
	'VARCHAR(23)'),
	@eventdata.value('(/EVENT_INSTANCE/EventType)[1]', 'SYSNAME'),
	@eventdata.value('(/EVENT_INSTANCE/LoginName)[1]', 'SYSNAME'),
	@eventdata.value('(/EVENT_INSTANCE/SchemaName)[1]', 'SYSNAME'),
	@eventdata.value('(/EVENT_INSTANCE/ObjectName)[1]', 'SYSNAME'),
	@eventdata.value('(/EVENT_INSTANCE/TargetObjectName)[1]', 'SYSNAME'),
	@eventdata.value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]','VARCHAR(4000)'),
	0,
	NULL,
	@eventdata);