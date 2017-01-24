CREATE TABLE [parallel].[ParallelExecution]
(
[SessionId] [uniqueidentifier] NOT NULL,
[SessionStatus] [int] NOT NULL DEFAULT ((0)),
[MaxDegreeOfParallelism] [int] NOT NULL DEFAULT ((1)),
[ContinueOnError] [bit] NOT NULL DEFAULT ((1)),
[LogLevel] [int] NOT NULL DEFAULT ((6)),
[PartitionStatement] [nvarchar] (max) NOT NULL,
[PartitionCommand] [nvarchar] (max) NOT NULL,
[DraftDate] [datetime] NULL,
[QueuedDate] [datetime] NULL,
[ProcessingDate] [datetime] NULL,
[FailedDate] [datetime] NULL,
[CompleteDate] [datetime] NULL,
[Comments] [nvarchar] (max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
ALTER TABLE [parallel].[ParallelExecution] ADD 
CONSTRAINT [PK_ParallelExecution] PRIMARY KEY CLUSTERED  ([SessionId]) ON [PRIMARY]
GO
