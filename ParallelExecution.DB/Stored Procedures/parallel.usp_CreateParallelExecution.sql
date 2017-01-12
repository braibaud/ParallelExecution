SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE procedure [parallel].[usp_CreateParallelExecution]
  @Status int = 0
 ,@MaxDegreeOfParallelism int = 1
 ,@ContinueOnError bit = 1
 ,@PartitionStatement nvarchar(max)
 ,@PartitionCommand nvarchar(max)
 ,@SessionId uniqueidentifier output
as
  begin
    if (@Status not in (0, 1))
      begin
        raiserror('Invalid value for parameter @Status', 10, 1) with nowait 
      end

    if (@PartitionStatement is null)
      begin
        raiserror('Invalid value for parameter @PartitionStatement', 10, 1) with nowait 
      end

    if (@PartitionCommand is null)
      begin
        raiserror('Invalid value for parameter @PartitionCommand', 10, 1) with nowait 
      end

    set @SessionId = newid()

    insert  into parallel.ParallelExecution
            (SessionId
            ,SessionStatus
            ,MaxDegreeOfParallelism
            ,ContinueOnError
            ,PartitionStatement
            ,PartitionCommand
            ,DraftDate
            ,QueuedDate
            ,ProcessingDate
            ,FailedDate
            ,CompleteDate)
    values  (@SessionId
            ,@Status
            ,@MaxDegreeOfParallelism
            ,@ContinueOnError
            ,@PartitionStatement
            ,@PartitionCommand
            ,(case when @Status = 0 then getutcdate()
                   else null
              end)
            ,(case when @Status = 1 then getutcdate()
                   else null
              end)
            ,null
            ,null
            ,null)

    declare @Comments nvarchar(1024) = 'SessionID: ' + convert(nvarchar(128), @SessionId)

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId,
      @PartitionId = null,
      @LogStatus = 5, /* Information = 5, Warning = 6, Error = 7, Critical = 8 */
      @LogDate = null, /* Logs event as 'now' */
      @Title = N'Session created via parallel.usp_CreateParallelExecution call.',
      @Comments = @Comments
  end

GO
