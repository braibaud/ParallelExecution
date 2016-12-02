using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace ParallelExecution
{
    /// <summary>
    /// 
    /// </summary>
    class Program
    {
        /// <summary>
        /// The app tracing
        /// </summary>
        public static TraceSource AppTracing = new TraceSource("AppTracing");

        /// <summary>
        /// The mail sender
        /// </summary>
        /// <value>
        /// The mail sender.
        /// </value>
        public static SmtpHelper MailSender { get; private set; }

        /// <summary>
        /// The application name
        /// </summary>
        private const string ApplicationName = "ParallelExecution";

        /// <summary>
        /// Gets the log path.
        /// </summary>
        /// <value>The log path.</value>
        public static string LogPath
        {
            get;
            private set;
        }

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
        /// Executes the query.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection">The connection.</param>
        /// <param name="query">The query.</param>
        /// <param name="function">The function.</param>
        /// <returns></returns>
        public static IEnumerable<T> ExecuteQuery<T>(
            SqlConnection connection,
            string query,
            Func<SqlDataReader, T> function)
        {
            using (SqlCommand cmd = new SqlCommand(query, connection))
            {
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        yield return function(reader);
                    }
                }
            }
        }

        private static int exitCode;
        private static ParallelExecutionConfiguration currentConfiguration;

        /// <summary>
        /// Main function.
        /// </summary>
        /// <param name="args">The args.</param>
        static int Main(string[] args)
        {
            exitCode = 0;
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            try
            {
                TraceSourceExtensionMethods.EventTraced += new EventHandler<TraceEventArgs>(TraceSourceExtensionMethods_EventTraced);

                LoadAllConfigurationKeys();
                SetupTracing();

                // setup emails...
                MailSender = new SmtpHelper(GetConfigurationValueAsString(AllConfigurationKeys.smtpServer.ToString()));

                using (MaintenanceSolutionEntities ctx = new MaintenanceSolutionEntities())
                {
                    AppTracing.TraceInformation("{0} - Calling stored procedure '{1}'...", DateTime.UtcNow, "dbo.usp_PullNextParallelExecution");
                    List<Guid?> results = ctx.usp_PullNextParallelExecution().ToList();

                    if (results != null &&
                        results.Any() &&
                        results.First().HasValue)
                    {
                        Guid result = results.First().Value;
                        AppTracing.TraceInformation("{0} - There is a job queued '{1}'...", DateTime.UtcNow, result.ToString());

                        try
                        {
                            currentConfiguration = ctx
                                    .ParallelExecutionConfigurations
                                    .Where(t => t.SessionId == result)
                                    .FirstOrDefault();

                            if (currentConfiguration != null)
                            {
                                string mainConnectionString = (ctx.Database.Connection as SqlConnection).ConnectionString;
                                AppTracing.TraceInformation("{0} - Config value 'mainConnectionString' set to {1}", DateTime.UtcNow, mainConnectionString);

                                int maxDegreeOfParallelism = currentConfiguration.MaxDegreeOfParallelism;
                                AppTracing.TraceInformation("{0} - Config value 'maxDegreeOfParallelism' set to {1}", DateTime.UtcNow, maxDegreeOfParallelism);

                                List<Dictionary<int, object>> values = null;

                                using (SqlConnection connection = new SqlConnection(mainConnectionString))
                                {
                                    connection.Open();

                                    values = ExecuteQuery(
                                        connection,
                                        currentConfiguration.PartitionStatement,
                                        t =>
                                        {
                                            return ExtractParameters(t);
                                        }).ToList();
                                }

                                AppTracing.TraceInformation("{0} - There are {1} partitions to process...", DateTime.UtcNow, values.Count);

                                try
                                {
                                    ParallelLoopResult ret = Parallel.ForEach(
                                        values,
                                        new ParallelOptions() { MaxDegreeOfParallelism = maxDegreeOfParallelism },
                                        value =>
                                        {
                                            AppTracing.TraceInformation("{0} - Processing {1}", DateTime.UtcNow, PrintParameters(value));

                                            try
                                            {
                                                using (SqlConnection connection = new SqlConnection(mainConnectionString))
                                                {
                                                    connection.Open();

                                                    using (SqlCommand cmd = new SqlCommand())
                                                    {
                                                        cmd.Connection = connection;
                                                        cmd.CommandTimeout = 3600;
                                                        cmd.CommandType = System.Data.CommandType.Text;
                                                        cmd.CommandText = currentConfiguration.PartitionCommand;

                                                        foreach (var entry in value)
                                                        {
                                                            cmd.Parameters.Add(
                                                                new SqlParameter(
                                                                    string.Format(
                                                                        "@Parameter{0}",
                                                                        entry.Key),
                                                                    entry.Value));
                                                        }

                                                        cmd.ExecuteNonQuery();
                                                        AppTracing.TraceInformation("{0} - Processing Complete for {1}", DateTime.UtcNow, PrintParameters(value));
                                                    }
                                                }
                                            }
                                            catch (SqlException se)
                                            {
                                                exitCode = 1;

                                                foreach (var entry in value)
                                                {
                                                    se.Data.Add(
                                                        string.Format(
                                                            "@Parameter{0}",
                                                            entry.Key),
                                                        entry.Value);
                                                }

                                                LogException(se);
                                            }
                                        });
                                }
                                catch (AggregateException ae)
                                {
                                    throw ae.Flatten();
                                }
                                finally
                                {
                                    if (currentConfiguration != null)
                                    {
                                        if (exitCode == 0)
                                        {
                                            currentConfiguration.SessionStatus = (int)SessionStatus.Complete;
                                            currentConfiguration.CompleteDate = DateTime.UtcNow;
                                        }
                                        else
                                        {
                                            currentConfiguration.SessionStatus = (int)SessionStatus.Failed;
                                            currentConfiguration.FailedDate = DateTime.UtcNow;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                AppTracing.TraceInformation("{0} - No job queued at the moment...", DateTime.UtcNow);
                            }
                        }
                        finally
                        {
                            if (ctx.ChangeTracker.HasChanges())
                            {
                                ctx.SaveChanges();
                            }

                            currentConfiguration = null;
                        }
                    }
                    else
                    {
                        AppTracing.TraceInformation("{0} - No job queued at the moment...", DateTime.UtcNow);
                    }
                }
            }
            catch (Exception e)
            {
                exitCode = 1;
                LogException(e);
            }

            return exitCode;
        }

        /// <summary>
        /// Extracts the parameters.
        /// </summary>
        /// <param name="t">The t.</param>
        /// <returns></returns>
        private static Dictionary<int, object> ExtractParameters(SqlDataReader t)
        {
            Dictionary<int, object> parameters = new Dictionary<int, object>();

            for (int i = 0; i < t.FieldCount; i++)
            {
                string column = t.GetName(i);

                if (column.StartsWith("Parameter", StringComparison.CurrentCultureIgnoreCase))
                {
                    string right = column.Substring(9);
                    int index;

                    if (int.TryParse(right, out index))
                    {
                        parameters.Add(index, t.GetValue(i));
                    }
                }
            }

            if (!parameters.Any())
            {
                throw new ApplicationException("No parameter found");
            }

            return parameters;
        }

        /// <summary>
        /// Prints the parameters.
        /// </summary>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        private static string PrintParameters(Dictionary<int, object> parameters)
        {
            return string.Join(", ", parameters.OrderBy(t => t.Key).Select(t => string.Format("Parameter{0}: '{1}'", t.Key, t.Value)));
        }

        /// <summary>
        /// Handles the InfoMessage event of the Program control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="System.Data.SqlClient.SqlInfoMessageEventArgs"/> instance containing the event data.</param>
        private static void Program_InfoMessage_ctx(object sender, SqlInfoMessageEventArgs e)
        {
            AppTracing.TraceInformation(e.Message);
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
            exitCode = 1;

            if (e.ExceptionObject is Exception)
            {
                LogException(e.ExceptionObject as Exception);
            }

            if (e.IsTerminating)
            {
                Environment.Exit(0);
            }
        }

        /// <summary>
        /// Handles the EventTraced event of the TraceSourceExtensionMethods control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="CommonFramework.ExtensionMethods.TraceEventArgs"/> instance containing the event data.</param>
        private static void TraceSourceExtensionMethods_EventTraced(
            object sender,
            TraceEventArgs e)
        {
            if (e.EventType <= TraceEventType.Warning)
            {
                XElement element = GetTraceEventArgsPrint(e);

                if (currentConfiguration != null)
                {
                    lock (currentConfiguration)
                    {
                        currentConfiguration.Comments += element.ToString(SaveOptions.DisableFormatting);
                    }
                }

                MailSender.SendTextMail(
                    GetConfigurationValueAsString(AllConfigurationKeys.fromAddress.ToString()),
                    string.Format(
                        "{0} -  Application issue",
                        ApplicationName),
                    element.ToString(),
                    GetTechnicalTeamEmails());
            }
        }

        /// <summary>
        /// Gets the assembly version.
        /// </summary>
        /// <value>
        /// The assembly version.
        /// </value>
        public static string AssemblyVersion
        {
            get
            {
                return Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
        }

        private static List<string> _technicalTeamEmails;

        /// <summary>
        /// Gets the technical team emails.
        /// </summary>
        /// <returns></returns>
        public static List<string> GetTechnicalTeamEmails()
        {
            if (_technicalTeamEmails == null)
            {
                string emails = GetConfigurationValueAsString(AllConfigurationKeys.technicalTeamEmails.ToString());

                if (!string.IsNullOrWhiteSpace(emails))
                {
                    _technicalTeamEmails = emails
                        .Split(',', ';')
                        .Where(t => !string.IsNullOrWhiteSpace(t))
                        .Select(t => t.ToLower().Trim())
                        .ToList();
                }
                else
                {
                    _technicalTeamEmails = Enumerable
                        .Empty<string>()
                        .ToList();
                }
            }

            return _technicalTeamEmails;
        }

        /// <summary>
        /// Gets the trace event arguments print.
        /// </summary>
        /// <param name="e">The <see cref="TraceEventArgs"/> instance containing the event data.</param>
        /// <returns></returns>
        public static XElement GetTraceEventArgsPrint(
            TraceEventArgs e)
        {
            XElement arg = new XElement("TraceEventArgs");

            if (e != null)
            {
                XElement exceptions = new XElement("Exceptions");

                foreach (Exception ex in e.Exceptions)
                {
                    exceptions.Add(GetExceptionPrint(ex));
                }

                arg.Add(exceptions);

                XElement details = new XElement("Details");
                details.Add(new XElement("MachineName", Environment.MachineName));
                details.Add(new XElement("Version", AssemblyVersion));
                details.Add(new XElement("Type", e.EventType.ToString().ToLower()));
                details.Add(new XElement("UserName", Environment.UserName));
                details.Add(new XElement("EventText", e.EventText));

                arg.Add(details);
            }

            return arg;
        }

        /// <summary>
        /// Gets the exception message.
        /// </summary>
        /// <param name="e">The e.</param>
        /// <returns></returns>
        public static XElement GetExceptionPrint(
            Exception e)
        {
            XElement exception = new XElement("Exception");

            if (e != null)
            {
                XElement exceptions = new XElement("Exceptions");

                if (e.Data != null)
                {
                    XElement data = new XElement("Data");

                    foreach (object key in e.Data.Keys)
                    {
                        XElement item = new XElement("Item");
                        item.Add(new XElement("Name", key));
                        item.Add(new XElement("Value", e.Data[key]));

                        data.Add(item);
                    }

                    exception.Add(data);
                }

                XElement details = new XElement("Details");
                details.Add(new XElement("Type", e.GetType()));
                details.Add(new XElement("Message", e.Message));
                details.Add(new XElement("Source", e.Source));
                details.Add(new XElement("StackTrace", e.StackTrace));

                if (e is SqlException)
                {
                    SqlException se = (e as SqlException);
                    details.Add(new XElement("ErrorCode", se.ErrorCode));
                    details.Add(new XElement("Number", se.Number));
                    details.Add(new XElement("Procedure", se.Procedure));
                    details.Add(new XElement("Server", se.Server));
                }

                exception.Add(details);

                if (e is AggregateException)
                {
                    AggregateException ae = (e as AggregateException);

                    foreach (Exception ee in ae.InnerExceptions)
                    {
                        exceptions.Add(GetExceptionPrint(ee));
                    }
                }

                if (e.InnerException != null)
                {
                    exceptions.Add(GetExceptionPrint(e.InnerException));
                }

                exception.Add(exceptions);
            }

            return exception;
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="ex">The ex.</param>
        public static void LogException(
            Exception ex)
        {
            if (AppTracing.Switch.ShouldTrace(TraceEventType.Critical))
            {
                if (ex != null)
                {
                    AppTracing.TraceCritical(ex.Message, ex);
                }
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
        /// Setups the tracing.
        /// </summary>
        private static void SetupTracing()
        {
            TextWriterTraceListener listener = (AppTracing.Listeners["GeneralListener"] as TextWriterTraceListener);

            if (listener != null)
            {
                listener.Flush();

                DateTime now = DateTime.UtcNow;

                string logFolder = Environment.ExpandEnvironmentVariables(
                    Path.Combine(
                        GetConfigurationValueAsString(AllConfigurationKeys.mainLogFolder.ToString()),
                        ApplicationName));

                if (!Directory.Exists(logFolder))
                {
                    Directory.CreateDirectory(logFolder);
                }

                LogPath = Path.Combine(
                    logFolder,
                    string.Format(
                        "{0:D4}.{1:D2}.{2:D2}-{3:D2}.{4:D2}.{5:D2}.log",
                        now.Year,
                        now.Month,
                        now.Day,
                        now.Hour,
                        now.Minute,
                        now.Second));

                listener.Writer = new StreamWriter(LogPath);
            }
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

    public enum SessionStatus : int
    {
        Draft = 0,
        Queued = 1,
        Processing = 2,
        Failed = 3,
        Complete = 4
    }
}
