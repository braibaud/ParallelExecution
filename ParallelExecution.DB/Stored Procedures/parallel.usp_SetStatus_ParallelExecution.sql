SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE procedure [parallel].[usp_SetStatus_ParallelExecution]
  @SessionID uniqueidentifier
 ,@SessionStatus int /* Draft = 0, Queued = 1, Processing = 2, Failed = 3, Complete = 4 */
 ,@Comments nvarchar(max)
as
  begin
    /* Draft */
    if (@SessionStatus = 0)
      begin
        update  parallel.ParallelExecution
        set     SessionStatus = @SessionStatus
               ,DraftDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
      end
    /* Queued */
    else if (@SessionStatus = 1)
      begin
        update  parallel.ParallelExecution
        set     SessionStatus = @SessionStatus
               ,QueuedDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
      end
    /* Processing */
    else if (@SessionStatus = 2)
      begin
        update  parallel.ParallelExecution
        set     SessionStatus = @SessionStatus
               ,ProcessingDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
      end
    /* Failed */
    else if (@SessionStatus = 3)
      begin
        update  parallel.ParallelExecution
        set     SessionStatus = @SessionStatus
               ,FailedDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
      end
    /* Complete */
    else if (@SessionStatus = 4)
      begin
        update  parallel.ParallelExecution
        set     SessionStatus = @SessionStatus
               ,CompleteDate = getutcdate()
               ,Comments = isnull(Comments + char(13) + char(10), '') + @Comments
        where   SessionId = @SessionID
      end


    declare @Comments_ nvarchar(1024) = 'SessionStatus: ' + isnull(convert(nvarchar(128), @SessionStatus), '<NULL>') + char(13) + char(10) +
                                        'Comments: ' + left(isnull(@Comments, '<NULL>'), 512)

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId,
      @PartitionId = null,
      @LogStatus = 5, /* Information = 5, Warning = 6, Error = 7, Critical = 8 */
      @LogDate = null, /* Logs event as 'now' */
      @Title = N'Session status updated via parallel.usp_SetStatus_ParallelExecution call.',
      @Comments = @Comments_
  end
GO
