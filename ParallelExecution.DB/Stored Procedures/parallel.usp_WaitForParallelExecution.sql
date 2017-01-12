SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
create procedure [parallel].[usp_WaitForParallelExecution]
  @SessionId uniqueidentifier
 ,@RunTimeoutInSeconds int = 120 /* 2 minutes timeout by default */
 ,@SessionStatus int = null output /* Draft = 0, Queued = 1, Processing = 2, Failed = 3, Complete = 4 */
as
  begin
    set nocount on;
    declare @start datetime = getutcdate();
    declare @message nvarchar(128);
    declare @poll_interval char(12) = '00:00:00.250';

    -- Verify that the session exists...
    if not exists ( select  1
                    from    parallel.ParallelExecution as pe
                    where   pe.SessionId = @SessionId )
      begin
        set @SessionStatus = null;
        set @message = 'Invalid Session-ID ' + convert(nvarchar(128), @SessionId);
        raiserror(@message, 16, 245);
        return 1;
      end;

    -- Wait for the Session to start processing...
    select  @SessionStatus = pe.SessionStatus
    from    parallel.ParallelExecution as pe with (nolock)
    where   pe.SessionId = @SessionId;

    while (@SessionStatus < 2 /* Processing */)
      begin
        set @message = 'Session ' + convert(nvarchar(128), @SessionId) + ' is not yet processing...';
        --print @message;
        waitfor delay @poll_interval;

        select  @SessionStatus = pe.SessionStatus
        from    parallel.ParallelExecution as pe with (nolock)
        where   pe.SessionId = @SessionId;

        if (datediff(second, @start, getutcdate()) < @RunTimeoutInSeconds)
          begin
            set @SessionStatus = null;
            set @message = 'WaitFor has timed-out';
            print @message;
            return 1;
          end;
      end;

    -- Wait for the Session to complete processing (either Failed or Complete)...
    select  @SessionStatus = pe.SessionStatus
    from    parallel.ParallelExecution as pe with (nolock)
    where   pe.SessionId = @SessionId;

    while (@SessionStatus = 2 /* Processing */)
      begin
        set @message = 'Session ' + convert(nvarchar(128), @SessionId) + ' is currently processing...';
        --print @message;
        waitfor delay @poll_interval;

        select  @SessionStatus = pe.SessionStatus
        from    parallel.ParallelExecution as pe with (nolock)
        where   pe.SessionId = @SessionId;

        if (datediff(second, @start, getutcdate()) < @RunTimeoutInSeconds)
          begin
            set @SessionStatus = null;
            set @message = 'WaitFor has timed-out';
            print @message;
            return 1;
          end;
      end;

    -- Wait until the Session to complete processing (either Failed or Complete)...
    select  @SessionStatus = pe.SessionStatus
    from    parallel.ParallelExecution as pe with (nolock)
    where   pe.SessionId = @SessionId;

    if (@SessionStatus = 3 /* Failed */)
      begin
        return 1;
      end;
    else
      if (@SessionStatus = 4 /* Complete */)
        begin
          return 0;
        end;
  end;
GO
