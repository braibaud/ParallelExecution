CREATE TABLE [parallel].[ParallelExecutionLog]
(
[LogId] [bigint] NOT NULL IDENTITY(1, 1),
[SessionId] [uniqueidentifier] NULL,
[PartitionId] [uniqueidentifier] NULL,
[LogStatus] [int] NULL,
[LogDate] [datetime] NULL,
[Title] [nvarchar] (128) NULL,
[Comments] [nvarchar] (1024) NULL
)
GO
ALTER TABLE [parallel].[ParallelExecutionLog] ADD CONSTRAINT [PK_ParallelExecutionLog] PRIMARY KEY CLUSTERED  ([LogId])
GO
