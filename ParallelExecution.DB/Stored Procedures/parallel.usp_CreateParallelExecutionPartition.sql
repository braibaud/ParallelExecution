
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO


CREATE procedure parallel.usp_CreateParallelExecutionPartition
  @SessionId uniqueidentifier
as
  begin
    declare @PartitionId uniqueidentifier = newid();

    insert  into parallel.ParallelExecutionPartition
            (SessionId
            ,PartitionId
            ,PartitionStatus
            ,QueuedDate)
    values  (@SessionId
            ,@PartitionId
            ,1
            ,getutcdate())
    
    select  @PartitionId as PartitionId


    declare @Comments nvarchar(1024) = 'PartitionID: ' + convert(nvarchar(128), @PartitionId)

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId
     ,@PartitionId = @PartitionId
     ,@LogStatus = 100 /* Information = 5, Warning = 6, Error = 7, Critical = 8, Important = 100 */
     ,@LogDate = null /* Logs event as 'now' */
     ,@Title = N'Partition created via parallel.usp_CreateParallelExecutionPartition call.'
     ,@Comments = @Comments
  end

GO
