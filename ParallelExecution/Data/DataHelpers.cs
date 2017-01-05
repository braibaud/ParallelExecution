using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace PE.Data
{
    /// <summary>
    /// 
    /// </summary>
    public static class DataHelpers
    {
        /// <summary>
        /// Gets the main connection string.
        /// </summary>
        /// <value>
        /// The main connection string.
        /// </value>
        public static string MainConnectionString
        {
            get
            {
                return Program.GetConfigurationValueAsString(AllConfigurationKeys.mainConnectionString.ToString());
            }
        }

        /// <summary>
        /// Gets the maximum degree of parallelism.
        /// </summary>
        /// <value>
        /// The maximum degree of parallelism.
        /// </value>
        public static int? MaxDegreeOfParallelism
        {
            get
            {
                return Program.GetConfigurationValueAsInteger(AllConfigurationKeys.maxDegreeOfParallelism.ToString());
            }
        }


        /// <summary>
        /// Executes the procedure and calls a function for each row returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The sql.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="function">The function.</param>
        /// <returns></returns>
        public static IEnumerable<T> Execute_Procedure_Reader<T>(
            SqlConnection connection,
            string sql,
            Dictionary<string, object> parameters,
            Func<SqlDataReader, T> function)
        {
            foreach (T item in Execute_Reader<T>(connection, sql, CommandType.StoredProcedure, parameters, function))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Executes the statement and calls a function for each row returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The sql.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="function">The function.</param>
        /// <returns></returns>
        public static IEnumerable<T> Execute_Statement_Reader<T>(
            SqlConnection connection,
            string sql,
            Dictionary<string, object> parameters,
            Func<SqlDataReader, T> function)
        {
            foreach (T item in Execute_Reader<T>(connection, sql, CommandType.Text, parameters, function))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Executes the sql and calls a function for each row returned.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The sql.</param>
        /// <param name="type">The type.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="function">The function.</param>
        /// <returns></returns>
        private static IEnumerable<T> Execute_Reader<T>(
            SqlConnection connection,
            string sql,
            CommandType type,
            Dictionary<string, object> parameters,
            Func<SqlDataReader, T> function)
        {
            if (connection == null)
            {
                using (SqlConnection conn = new SqlConnection(MainConnectionString))
                {
                    conn.Open();

                    foreach (T item in Execute_Reader<T>(conn, sql, type, parameters, function))
                    {
                        yield return item;
                    }
                }
            }
            else
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandType = type;
                    cmd.CommandTimeout = 3600;
                    cmd.CommandText = sql;

                    if (parameters != null)
                    {
                        foreach (string key in parameters.Keys)
                        {
                            cmd.Parameters.AddWithValue(
                                key,
                                ConvertForParameter(parameters[key]));
                        }
                    }

                    using (SqlDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return function(reader);
                        }
                    }
                }
            }
        }


        /// <summary>
        /// Executes the statement and return nothing.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="parameters">The parameters.</param>
        public static void Execute_Statement_NonQuery(
            SqlConnection connection,
            string sql,
            Dictionary<string, object> parameters)
        {
            Execute_NonQuery(
                connection,
                sql,
                CommandType.Text,
                parameters);
        }

        /// <summary>
        /// Executes the procedure and return nothing.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The SQL.</param>
        /// <param name="parameters">The parameters.</param>
        public static void Execute_Procedure_NonQuery(
            SqlConnection connection,
            string sql,
            Dictionary<string, object> parameters)
        {
            Execute_NonQuery(
                connection,
                sql,
                CommandType.StoredProcedure,
                parameters);
        }

        /// <summary>
        /// Executes the sql and return nothing.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sql">The sql.</param>
        /// <param name="type">The type.</param>
        /// <param name="parameters">The parameters.</param>
        private static void Execute_NonQuery(
            SqlConnection connection,
            string sql,
            CommandType type,
            Dictionary<string, object> parameters)
        {
            if (connection == null)
            {
                using (SqlConnection conn = new SqlConnection(MainConnectionString))
                {
                    conn.Open();

                    Execute_NonQuery(
                        conn,
                        sql,
                        type,
                        parameters);
                }
            }
            else
            {
                using (SqlCommand cmd = new SqlCommand())
                {
                    cmd.Connection = connection;
                    cmd.CommandType = type;
                    cmd.CommandTimeout = 3600;
                    cmd.CommandText = sql;

                    if (parameters != null)
                    {
                        foreach (string key in parameters.Keys)
                        {
                            cmd.Parameters.AddWithValue(
                                key,
                                ConvertForParameter(parameters[key]));
                        }
                    }

                    cmd.ExecuteNonQuery();
                }
            }
        }


        /// <summary>
        /// Converts the value for the parameter.
        /// </summary>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public static object ConvertForParameter(
            object value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }
            else
            {
                return value;
            }
        }

        /// <summary>
        /// Logs a parallel execution event.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sessionID">The session identifier.</param>
        /// <param name="partitionID">The partition identifier.</param>
        /// <param name="logStatus">The log status.</param>
        /// <param name="title">The title.</param>
        /// <param name="comments">The comments.</param>
        public static void usp_LogParallelExecutionEvent(
            SqlConnection connection,
            Guid? sessionID,
            Guid? partitionID,
            ParallelExecutionEventStatus logStatus,
            string title,
            string comments)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("@SessionId", ConvertForParameter(sessionID));
            parameters.Add("@PartitionId", ConvertForParameter(partitionID));
            parameters.Add("@LogStatus", (int)logStatus);
            parameters.Add("@LogDate", ConvertForParameter(null));
            parameters.Add("@Title", ConvertForParameter(title));
            parameters.Add("@Comments", ConvertForParameter(comments));

            Execute_Procedure_NonQuery(
                connection,
                "parallel.usp_LogParallelExecutionEvent",
                parameters);
        }

        /// <summary>
        /// Pulls the next parallel execution.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <returns></returns>
        public static ParallelExecution usp_PullNextParallelExecution(
            SqlConnection connection)
        {
            return Execute_Procedure_Reader<ParallelExecution>(
                connection,
                "parallel.usp_PullNextParallelExecution",
                null,
                reader =>
                {
                    return new ParallelExecution()
                    {
                        SessionId = reader.GetGuid(reader.GetOrdinal("SessionId")),
                        SessionStatus = (SessionPartitionStatus)reader.GetInt32(reader.GetOrdinal("SessionStatus")),
                        MaxDegreeOfParallelism = reader.GetInt32(reader.GetOrdinal("MaxDegreeOfParallelism")),
                        ContinueOnError = reader.GetBoolean(reader.GetOrdinal("ContinueOnError")),
                        PartitionStatement = reader.GetString(reader.GetOrdinal("PartitionStatement")),
                        PartitionCommand = reader.GetString(reader.GetOrdinal("PartitionCommand")),
                        DraftDate = (reader.IsDBNull(reader.GetOrdinal("DraftDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("DraftDate"))),
                        QueuedDate = (reader.IsDBNull(reader.GetOrdinal("QueuedDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("QueuedDate"))),
                        ProcessingDate = (reader.IsDBNull(reader.GetOrdinal("ProcessingDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("ProcessingDate"))),
                        FailedDate = (reader.IsDBNull(reader.GetOrdinal("FailedDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("FailedDate"))),
                        CompleteDate = (reader.IsDBNull(reader.GetOrdinal("CompleteDate")) ? (DateTime?)null : reader.GetDateTime(reader.GetOrdinal("CompleteDate"))),
                        Comments = (reader.IsDBNull(reader.GetOrdinal("Comments")) ? null : reader.GetString(reader.GetOrdinal("Comments")))
                    };
                })
                .FirstOrDefault();
        }

        /// <summary>
        /// Usp_s the create parallel execution partition.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sessionID">The session identifier.</param>
        /// <returns></returns>
        public static Guid usp_CreateParallelExecutionPartition(
            SqlConnection connection,
            Guid sessionID)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("@SessionId", sessionID);

            return Execute_Procedure_Reader<Guid>(
                connection,
                "parallel.usp_CreateParallelExecutionPartition",
                parameters,
                reader =>
                {
                    return reader.GetGuid(reader.GetOrdinal("PartitionId"));
                })
                .ToList()
                .First();
        }

        /// <summary>
        /// Creates a parallel execution partition parameter.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sessionID">The session identifier.</param>
        /// <param name="partitionID">The partition identifier.</param>
        /// <param name="parameterIndex">Index of the parameter.</param>
        /// <param name="parameterValue">The parameter value.</param>
        public static void usp_CreateParallelExecutionPartitionParameter(
            SqlConnection connection,
            Guid sessionID,
            Guid partitionID,
            int parameterIndex,
            object parameterValue)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("@SessionId", sessionID);
            parameters.Add("@PartitionId", partitionID);
            parameters.Add("@ParameterIndex", parameterIndex);
            parameters.Add("@ParameterValue", ConvertForParameter(parameterValue));


            Execute_Procedure_NonQuery(
                connection,
                "parallel.usp_CreateParallelExecutionPartitionParameter",
                parameters);
        }

        /// <summary>
        /// Update the parallel execution status.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sessionID">The session identifier.</param>
        /// <param name="sessionStatus">The session status.</param>
        /// <param name="comments">The comments.</param>
        public static void usp_SetStatus_ParallelExecution(
            SqlConnection connection,
            Guid sessionID,
            SessionPartitionStatus sessionStatus,
            string comments)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("@SessionId", sessionID);
            parameters.Add("@SessionStatus", (int)sessionStatus);
            parameters.Add("@Comments", ConvertForParameter(comments));

            Execute_Procedure_NonQuery(
                connection,
                "parallel.usp_SetStatus_ParallelExecution",
                parameters);
        }

        /// <summary>
        /// Update the parallel execution partition status.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="sessionID">The session identifier.</param>
        /// <param name="partitionID">The partition identifier.</param>
        /// <param name="partitionStatus">The partition status.</param>
        /// <param name="comments">The comments.</param>
        public static void usp_SetStatus_ParallelExecutionPartition(
            SqlConnection connection,
            Guid sessionID,
            Guid partitionID,
            SessionPartitionStatus partitionStatus,
            string comments)
        {
            Dictionary<string, object> parameters = new Dictionary<string, object>();
            parameters.Add("@SessionId", sessionID);
            parameters.Add("@PartitionId", partitionID);
            parameters.Add("@PartitionStatus", (int)partitionStatus);
            parameters.Add("@Comments", ConvertForParameter(comments));

            Execute_Procedure_NonQuery(
                connection,
                "parallel.usp_SetStatus_ParallelExecutionPartition",
                parameters);
        }

        /// <summary>
        /// Extracts the parameters.
        /// </summary>
        /// <param name="reader">The SQL Data Reader</param>
        /// <returns></returns>
        private static Partition ExtractParameters(
            SqlDataReader reader)
        {
            Partition partition = new Partition();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string column = reader.GetName(i);

                if (column.StartsWith("Parameter", StringComparison.CurrentCultureIgnoreCase))
                {
                    string right = column.Substring(9);
                    int index;

                    if (int.TryParse(right, out index))
                    {
                        partition.Parameters.Add(index, reader.GetValue(i));
                    }
                }
            }

            if (!partition.Parameters.Any())
            {
                throw new ApplicationException("No parameter found");
            }

            return partition;
        }

        /// <summary>
        /// Gets the partitions.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="partitionStatement">The partition statement.</param>
        /// <returns></returns>
        public static List<Partition> GetPartitions(
            SqlConnection connection,
            string partitionStatement)
        {
            return Execute_Statement_Reader(
                connection,
                partitionStatement,
                null,
                ExtractParameters)
            .ToList();
        }
    }
}
