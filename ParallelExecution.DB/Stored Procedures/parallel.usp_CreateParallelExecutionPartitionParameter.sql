SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO


CREATE procedure [parallel].[usp_CreateParallelExecutionPartitionParameter]
  @SessionId uniqueidentifier
 ,@PartitionId uniqueidentifier
 ,@ParameterIndex int
 ,@ParameterValue sql_variant
as
  begin
    insert  into parallel.ParallelExecutionPartitionParameter
            (SessionId
            ,PartitionId
            ,ParameterIndex
            ,ParameterValue)
    values  (@SessionId
            ,@PartitionId
            ,@ParameterIndex
            ,@ParameterValue)


    declare @Comments nvarchar(1024) = 'ParameterIndex: ' + convert(nvarchar(128), @ParameterIndex) + char(13) + char(10) +
                                       'ParameterValue: ' + isnull(convert(nvarchar(128), @ParameterValue), '<NULL>')

    exec parallel.usp_LogParallelExecutionEvent
      @SessionId = @SessionId,
      @PartitionId = @PartitionId,
      @LogStatus = 5, /* Information = 5, Warning = 6, Error = 7, Critical = 8 */
      @LogDate = null, /* Logs event as 'now' */
      @Title = N'Partition parameter created via parallel.usp_CreateParallelExecutionPartitionParameter call.',
      @Comments = @Comments
  end

GO
