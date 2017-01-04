namespace PE.Data
{

    /// <summary>
    /// 
    /// </summary>
    public enum SessionPartitionStatus : int
    {
        Draft = 0,
        Queued = 1,
        Processing = 2,
        Failed = 3,
        Complete = 4
    }
}