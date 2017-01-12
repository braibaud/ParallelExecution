CREATE TABLE [parallel].[ParallelExecutionPartitionParameter]
(
[SessionId] [uniqueidentifier] NOT NULL,
[PartitionId] [uniqueidentifier] NOT NULL,
[ParameterIndex] [int] NOT NULL,
[ParameterValue] [sql_variant] NULL
)
GO
ALTER TABLE [parallel].[ParallelExecutionPartitionParameter] ADD CONSTRAINT [PK_ParallelExecutionPartitionParameter] PRIMARY KEY CLUSTERED  ([SessionId], [PartitionId], [ParameterIndex])
GO
ALTER TABLE [parallel].[ParallelExecutionPartitionParameter] ADD CONSTRAINT [FK_ParallelExecutionPartitionParameter_ParallelExecutionPartition] FOREIGN KEY ([SessionId], [PartitionId]) REFERENCES [parallel].[ParallelExecutionPartition] ([SessionId], [PartitionId])
GO
