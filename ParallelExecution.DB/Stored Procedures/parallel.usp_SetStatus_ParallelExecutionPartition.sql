
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE procedure parallel.usp_SetStatus_ParallelExecutionPartition
  @SessionID uniqueidentifier
 ,@PartitionID uniqueidentifier
 ,@PartitionStatus int /* Draft = 0, Queued = 1, Processing = 2, Failed = 3, Complete = 4 */
 ,@Comments nvarchar(max)
as
  begin
    /* Queued */
    if (@PartitionStatus = 1)
      begin
        update  parallel.ParallelExecutionPartition
        set     PartitionStatus = @PartitionStatus
               ,QueuedDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
                and PartitionId = @PartitionID
      end
    /* Processing */
    else
      if (@PartitionStatus = 2)
        begin
          update  parallel.ParallelExecutionPartition
          set     PartitionStatus = @PartitionStatus
                 ,ProcessingDate = getutcdate()
                 ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
          where   SessionId = @SessionID
                  and PartitionId = @PartitionID
        end
    /* Failed */
      else
        if (@PartitionStatus = 3)
          begin
            update  parallel.ParallelExecutionPartition
            set     PartitionStatus = @PartitionStatus
                   ,FailedDate = getutcdate()
                   ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
            where   SessionId = @SessionID
                    and PartitionId = @PartitionID
          end
    /* Complete */
        else
          if (@PartitionStatus = 4)
            begin
              update  parallel.ParallelExecutionPartition
              set     PartitionStatus = @PartitionStatus
                     ,CompleteDate = getutcdate()
                     ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
              where   SessionId = @SessionID
                      and PartitionId = @PartitionID
            end


    declare @Comments_ nvarchar(1024) = 'PartitionStatus: ' + isnull(convert(nvarchar(128), @PartitionStatus), '<NULL>') + char(13) + char(10) + 'Comments: '
      + left(isnull(@Comments, '<NULL>'), 512)

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId
     ,@PartitionId = @PartitionID
     ,@LogStatus = 100 /* Information = 5, Warning = 6, Error = 7, Critical = 8, Important = 100 */
     ,@LogDate = null /* Logs event as 'now' */
     ,@Title = N'Session status updated via parallel.usp_SetStatus_ParallelExecutionPartition call.'
     ,@Comments = @Comments_
  end
GO
