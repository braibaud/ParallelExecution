namespace PE.Data
{

    public partial class ParallelExecutionPartition
    {
        public System.Guid SessionId { get; set; }
        public System.Guid PartitionId { get; set; }
        public SessionPartitionStatus PartitionStatus { get; set; }
    }

}