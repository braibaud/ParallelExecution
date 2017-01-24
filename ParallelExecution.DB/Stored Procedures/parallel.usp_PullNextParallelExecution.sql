
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO

CREATE procedure parallel.usp_PullNextParallelExecution
as
  begin
    declare @SessionId uniqueidentifier = null;

    begin try
      begin transaction;

      if (exists ( select 1
                   from   parallel.ParallelExecution
                   where  SessionStatus = 1 ))
        begin
          create table #temporary
          (
           SessionId uniqueidentifier
          );
      
          update  m
          set     m.SessionStatus = 2
                 ,m.ProcessingDate = getutcdate()
          output  inserted.SessionId
                  into #temporary
          from    (select top 1
                          pe.*
                   from   parallel.ParallelExecution as pe
                   where  pe.SessionStatus = 1
                   order by pe.QueuedDate desc) as m;

          if (exists ( select 1
                       from   #temporary ))
            begin
              select  @SessionId = t.SessionId
              from    #temporary as t
            end
          
        end

      commit transaction;
    end try
    begin catch 
      if (@@TRANCOUNT > 0)
        begin
          rollback transaction 
        end 

      declare @ErrorNumber int = error_number();
      declare @ErrorLine int = error_line();
      declare @ErrorMessage nvarchar(4000) = error_message();
      declare @ErrorSeverity int = error_severity();
      declare @ErrorState int = error_state();
 
      set @SessionId = null;
      raiserror(@ErrorMessage, @ErrorSeverity, @ErrorState);

    end catch

    select  p.SessionId
           ,p.SessionStatus
           ,p.MaxDegreeOfParallelism
           ,p.ContinueOnError
           ,p.PartitionStatement
           ,p.PartitionCommand
           ,p.DraftDate
           ,p.QueuedDate
           ,p.ProcessingDate
           ,p.FailedDate
           ,p.CompleteDate
           ,p.Comments
    from    parallel.ParallelExecution as p
    where   p.SessionId = @SessionId


    declare @Comments nvarchar(1024) = 'SessionID: ' + isnull(convert(nvarchar(128), @SessionId), '<NULL>')

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId
     ,@PartitionId = null
     ,@LogStatus = 100 /* Information = 5, Warning = 6, Error = 7, Critical = 8, Important = 100 */
     ,@LogDate = null /* Logs event as 'now' */
     ,@Title = N'Session pulled via parallel.usp_PullNextParallelExecution call.'
     ,@Comments = @Comments
  end

GO
