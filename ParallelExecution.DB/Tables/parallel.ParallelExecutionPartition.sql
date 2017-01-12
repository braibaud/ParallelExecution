CREATE TABLE [parallel].[ParallelExecutionPartition]
(
[SessionId] [uniqueidentifier] NOT NULL,
[PartitionId] [uniqueidentifier] NOT NULL,
[PartitionStatus] [int] NOT NULL,
[QueuedDate] [datetime] NULL,
[ProcessingDate] [datetime] NULL,
[FailedDate] [datetime] NULL,
[CompleteDate] [datetime] NULL,
[Comments] [nvarchar] (max) NULL
)
GO
ALTER TABLE [parallel].[ParallelExecutionPartition] ADD CONSTRAINT [PK_ParallelExecutionPartition] PRIMARY KEY CLUSTERED  ([SessionId], [PartitionId])
GO
ALTER TABLE [parallel].[ParallelExecutionPartition] ADD CONSTRAINT [FK_ParallelExecutionPartition_ParallelExecution] FOREIGN KEY ([SessionId]) REFERENCES [parallel].[ParallelExecution] ([SessionId])
GO
