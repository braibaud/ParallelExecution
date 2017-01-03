namespace PE.Data
{

    public partial class ParallelExecutionPartitionParameter
    {
        public System.Guid SessionId { get; set; }
        public System.Guid PartitionId { get; set; }
        public int ParameterIndex { get; set; }
        public object ParameterValue { get; set; }
    }

}