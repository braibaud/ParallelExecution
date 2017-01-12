using PE.Data;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace PE
{
    /// <summary>
    /// 
    /// </summary>
    class Program
    {
        /// <summary>
        /// The application name
        /// </summary>
        private const string ApplicationName = "ParallelExecution (PE)";

        /// <summary>
        /// Gets or sets the configuration keys.
        /// </summary>
        /// <value>The configuration keys.</value>
        public static Dictionary<string, string> ConfigurationKeys
        {
            get;
            private set;
        }

        /// <summary>
        /// The current configuration
        /// </summary>
        private static ParallelExecution currentConfiguration;

        /// <summary>
        /// Gets or sets the exit code.
        /// </summary>
        /// <value>
        /// The exit code.
        /// </value>
        public static int ExitCode { get; set; }

        /// <summary>
        /// Main function.
        /// </summary>
        /// <param name="args">The args.</param>
        static int Main(
            string[] args)
        {
            ExitCode = 0;
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            try
            {
                LoadAllConfigurationKeys();

                currentConfiguration = DataHelpers.usp_PullNextParallelExecution(null);

                if (currentConfiguration != null)
                {
                    try
                    {
                        // Max Number of cores...
                        int nbCores = (DataHelpers.MaxDegreeOfParallelism ?? 8);

                        if ((currentConfiguration.MaxDegreeOfParallelism < -1) ||
                            (currentConfiguration.MaxDegreeOfParallelism == 0) ||
                            (currentConfiguration.MaxDegreeOfParallelism > nbCores))
                        {
                            currentConfiguration.MaxDegreeOfParallelism = nbCores;
                        }

                        List<Partition> partitions = DataHelpers.GetPartitions(
                            null,
                            currentConfiguration.PartitionStatement);

                        foreach (Partition partition in partitions)
                        {
                            partition.SessionId = currentConfiguration.SessionId;

                            partition.PartitionId = DataHelpers.usp_CreateParallelExecutionPartition(
                                null,
                                currentConfiguration.SessionId);

                            foreach (KeyValuePair<int, object> parameter in partition.Parameters)
                            {
                                DataHelpers.usp_CreateParallelExecutionPartitionParameter(
                                    null,
                                    currentConfiguration.SessionId,
                                    partition.PartitionId,
                                    parameter.Key,
                                    parameter.Value);
                            }
                        }

                        ParallelLoopResult ret = Parallel.ForEach(
                            partitions,
                            new ParallelOptions() { MaxDegreeOfParallelism = currentConfiguration.MaxDegreeOfParallelism },
                            (partition, state) =>
                            {
                                bool success = false;
                                string error = null;

                                try
                                {
                                    DataHelpers.usp_SetStatus_ParallelExecutionPartition(
                                        null,
                                        currentConfiguration.SessionId,
                                        partition.PartitionId,
                                        SessionPartitionStatus.Processing,
                                        null);

                                    using (SqlConnection connection = new SqlConnection(DataHelpers.MainConnectionString))
                                    {
                                        connection.Open();
                                        connection.FireInfoMessageEventOnUserErrors = false;
                                        connection.InfoMessage += (sender, message) =>
                                                                  {
                                                                      DataHelpers.usp_LogParallelExecutionEvent(
                                                                          null,
                                                                          currentConfiguration.SessionId,
                                                                          partition.PartitionId,
                                                                          ParallelExecutionEventStatus.Information,
                                                                          "InfoMessage",
                                                                          message.Message);
                                                                  };

                                        DataHelpers.Execute_Statement_NonQuery(
                                            connection,
                                            currentConfiguration.PartitionCommand,
                                            partition.GetSqlParameters());
                                    }

                                    success = true;
                                }
                                catch (Exception e)
                                {
                                    error = e.Message;

                                    DataHelpers.usp_LogParallelExecutionEvent(
                                        null,
                                        currentConfiguration.SessionId,
                                        partition.PartitionId,
                                        ParallelExecutionEventStatus.Error,
                                        e.Message,
                                        e.ToString());
                                }
                                finally
                                {
                                    partition.PartitionStatus = (success ? SessionPartitionStatus.Complete : SessionPartitionStatus.Failed);

                                    DataHelpers.usp_SetStatus_ParallelExecutionPartition(
                                        null,
                                        currentConfiguration.SessionId,
                                        partition.PartitionId,
                                        (success ? SessionPartitionStatus.Complete : SessionPartitionStatus.Failed),
                                        error);
                                }

                                if (!success &&
                                    !currentConfiguration.ContinueOnError)
                                {
                                    DataHelpers.usp_LogParallelExecutionEvent(
                                        null,
                                        currentConfiguration.SessionId,
                                        partition.PartitionId,
                                        ParallelExecutionEventStatus.Information,
                                        "Stopping",
                                        "Partition processing failed and the session is configured to stop on first partition failure (ContinueOnError = false).");

                                    state.Stop();
                                }
                            });

                        if (!ret.IsCompleted)
                        {
                            currentConfiguration.SessionStatus = SessionPartitionStatus.Failed;
                        }
                        else
                        {
                            if (partitions.Where(t => t.PartitionStatus != SessionPartitionStatus.Complete).Any())
                            {
                                // In theory, we should never end up there, but...
                                currentConfiguration.SessionStatus = SessionPartitionStatus.Failed;
                            }
                            else
                            {
                                currentConfiguration.SessionStatus = SessionPartitionStatus.Complete;
                            }
                        }
                    }
                    finally
                    {
                        // Make sure we are setting the status to either Failed or Complete...
                        if (currentConfiguration.SessionStatus < SessionPartitionStatus.Failed)
                        {
                            currentConfiguration.SessionStatus = SessionPartitionStatus.Failed;
                        }

                        // Update overall status...
                        DataHelpers.usp_SetStatus_ParallelExecution(
                            null,
                            currentConfiguration.SessionId,
                            currentConfiguration.SessionStatus,
                            null);
                    }
                }
            }
            catch (Exception e)
            {
                TryAndReportException(e);
            }

            return ExitCode;
        }

        /// <summary>
        /// Tries the and report exception.
        /// </summary>
        /// <param name="e">The e.</param>
        private static void TryAndReportException(
            Exception e)
        {
            Guid? session = (currentConfiguration != null ? (Guid?)currentConfiguration.SessionId : null);

            DataHelpers.usp_LogParallelExecutionEvent(
                null,
                session,
                null,
                ParallelExecutionEventStatus.Error,
                e.Message,
                e.ToString());
        }

        /// <summary>
        /// Handles the UnhandledException event of the CurrentDomain control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="System.UnhandledExceptionEventArgs"/> instance containing the event data.</param>
        private static void CurrentDomain_UnhandledException(
            object sender,
            UnhandledExceptionEventArgs e)
        {
            ExitCode = 1;

            if (e.ExceptionObject is Exception)
            {
                TryAndReportException(
                    e.ExceptionObject as Exception);
            }

            if (e.IsTerminating)
            {
                Environment.Exit(0);
            }
        }

        /// <summary>
        /// Loads all configuration keys.
        /// </summary>
        private static void LoadAllConfigurationKeys()
        {
            ConfigurationKeys = new Dictionary<string, string>(
                StringComparer.InvariantCultureIgnoreCase);

            IEnumerable<string> configFile = ConfigurationManager.AppSettings.OfType<string>();

            foreach (string key in configFile)
            {
                ConfigurationKeys.Add(key, ConfigurationManager.AppSettings[key]);
            }
        }

        /// <summary>
        /// Gets the parameter value as integer.
        /// </summary>
        /// <param name="paramName">Name of the parameter.</param>
        /// <param name="args">The arguments.</param>
        /// <returns></returns>
        private static int? GetParamValueAsInteger(
            string paramName,
            string[] args)
        {
            int? result = null;
            int value;

            if (int.TryParse(GetParamValue(paramName, args), out value))
            {
                result = value;
            }

            return result;
        }

        /// <summary>
        /// Gets the parameter value boolean.
        /// </summary>
        /// <param name="paramName">Name of the parameter.</param>
        /// <param name="args">The arguments.</param>
        /// <returns></returns>
        private static bool? GetParamValueAsBoolean(
            string paramName,
            string[] args)
        {
            bool? result = null;
            bool value;

            if (bool.TryParse(GetParamValue(paramName, args), out value))
            {
                result = value;
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="paramName"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        private static string GetParamValue(
            string paramName,
            string[] args)
        {
            string mode = string.Empty;
            string delimiter = string.Format("/{0}:", paramName);

            if ((args != null) &&
                (args.Length > 0))
            {
                foreach (string arg in args)
                {
                    string argument = arg.Trim();

                    if (!string.IsNullOrEmpty(argument))
                    {
                        if (argument.StartsWith(delimiter, StringComparison.CurrentCultureIgnoreCase))
                        {
                            string modeArgument = argument.Substring(delimiter.Length);

                            if (!string.IsNullOrEmpty(modeArgument))
                            {
                                mode = modeArgument;
                            }

                            break;
                        }
                    }
                }
            }

            return mode;
        }

        /// <summary>
        /// Gets the configuration value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static string GetConfigurationValueAsString(
            string key)
        {
            string value = null;

            if (ConfigurationKeys.ContainsKey(key))
            {
                value = ConfigurationKeys[key];
            }

            return value;
        }

        /// <summary>
        /// Gets the configuration value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static int? GetConfigurationValueAsInteger(
            string key)
        {
            int? value = null;

            if (ConfigurationKeys.ContainsKey(key))
            {
                int configValue;

                if (int.TryParse(ConfigurationKeys[key], out configValue))
                {
                    value = configValue;
                }
            }

            return value;
        }

        /// <summary>
        /// Gets the configuration value as boolean.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public static bool? GetConfigurationValueAsBoolean(
            string key)
        {
            bool? value = null;

            if (ConfigurationKeys.ContainsKey(key))
            {
                bool configValue;

                if (bool.TryParse(ConfigurationKeys[key], out configValue))
                {
                    value = configValue;
                }
            }

            return value;
        }
    }
}
