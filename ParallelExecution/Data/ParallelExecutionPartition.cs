using System;

namespace PE.Data
{

    public partial class ParallelExecutionPartition
    {
        public System.Guid SessionId { get; set; }
        public System.Guid PartitionId { get; set; }
        public SessionPartitionStatus PartitionStatus { get; set; }
        public Nullable<System.DateTime> QueuedDate { get; set; }
        public Nullable<System.DateTime> ProcessingDate { get; set; }
        public Nullable<System.DateTime> FailedDate { get; set; }
        public Nullable<System.DateTime> CompleteDate { get; set; }
        public string Comments { get; set; }
    }

}