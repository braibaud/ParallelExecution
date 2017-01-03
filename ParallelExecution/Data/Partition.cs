using System;
using System.Collections.Generic;

namespace PE.Data
{
    /// <summary>
    /// 
    /// </summary>
    public class Partition
    {
        /// <summary>
        /// Gets or sets the partition identifier.
        /// </summary>
        /// <value>
        /// The partition identifier.
        /// </value>
        public Guid PartitionId { get; set; }

        /// <summary>
        /// The parameters
        /// </summary>
        public Dictionary<int, object> Parameters = new Dictionary<int, object>();

        /// <summary>
        /// Gets the SQL parameters.
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, object> GetSqlParameters()
        {
            Dictionary<string, object> dico = new Dictionary<string, object>();

            foreach (KeyValuePair<int, object> item in Parameters)
            {
                dico.Add(
                    string.Format("@Parameter{0}", item.Key),
                    DataHelpers.ConvertForParameter(item.Value));
            }

            return dico;
        }
    }
}
