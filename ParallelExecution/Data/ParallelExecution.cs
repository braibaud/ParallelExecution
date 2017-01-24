using System;

namespace PE.Data
{
    public partial class ParallelExecution
    {
        public System.Guid SessionId { get; set; }
        public SessionPartitionStatus SessionStatus { get; set; }
        public int MaxDegreeOfParallelism { get; set; }
        public bool ContinueOnError { get; set; }
        public ParallelExecutionEventStatus LogLevel { get; set; }
        public string PartitionStatement { get; set; }
        public string PartitionCommand { get; set; }
        public Nullable<System.DateTime> DraftDate { get; set; }
        public Nullable<System.DateTime> QueuedDate { get; set; }
        public Nullable<System.DateTime> ProcessingDate { get; set; }
        public Nullable<System.DateTime> FailedDate { get; set; }
        public Nullable<System.DateTime> CompleteDate { get; set; }
        public string Comments { get; set; }
    }
}
