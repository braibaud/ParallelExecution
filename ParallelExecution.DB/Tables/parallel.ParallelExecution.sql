CREATE TABLE [parallel].[ParallelExecution]
(
[SessionId] [uniqueidentifier] NOT NULL,
[SessionStatus] [int] NOT NULL CONSTRAINT [DF__ParallelE__Sessi__06CD04F7] DEFAULT ((0)),
[MaxDegreeOfParallelism] [int] NOT NULL CONSTRAINT [DF__ParallelE__MaxDe__07C12930] DEFAULT ((1)),
[ContinueOnError] [bit] NOT NULL CONSTRAINT [DF_ParallelExecution_ContinueOnError] DEFAULT ((1)),
[PartitionStatement] [nvarchar] (max) NOT NULL,
[PartitionCommand] [nvarchar] (max) NOT NULL,
[DraftDate] [datetime] NULL,
[QueuedDate] [datetime] NULL,
[ProcessingDate] [datetime] NULL,
[FailedDate] [datetime] NULL,
[CompleteDate] [datetime] NULL,
[Comments] [nvarchar] (max) NULL
)
GO
ALTER TABLE [parallel].[ParallelExecution] ADD CONSTRAINT [PK_ParallelExecution] PRIMARY KEY CLUSTERED  ([SessionId])
GO
