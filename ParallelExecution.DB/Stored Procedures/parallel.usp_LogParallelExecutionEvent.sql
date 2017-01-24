
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE procedure parallel.usp_LogParallelExecutionEvent
  @SessionId uniqueidentifier
 ,@PartitionId uniqueidentifier
 ,@LogStatus int
 ,@LogDate datetime
 ,@Title nvarchar(128)
 ,@Comments nvarchar(1024)
as
  begin
    if (exists ( select 1
                 from   parallel.ParallelExecution
                 where  SessionId = @SessionId
                        and LogLevel <= @LogStatus ))
      begin
        insert  into parallel.ParallelExecutionLog
                (SessionId
                ,PartitionId
                ,LogStatus
                ,LogDate
                ,Title
                ,Comments)
        values  (@SessionId
                ,@PartitionId
                ,@LogStatus
                ,isnull(@LogDate, getutcdate())
                ,@Title
                ,@Comments)
      end
  end

GO
