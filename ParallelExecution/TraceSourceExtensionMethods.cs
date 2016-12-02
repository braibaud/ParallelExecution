using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace ParallelExecution
{
    /// <summary>
    /// 
    /// </summary>
    public static class TraceSourceExtensionMethods
    {
        /// <summary>
        /// Occurs when [event traced].
        /// </summary>
        public static event EventHandler<TraceEventArgs> EventTraced;

        #region Trace register
        /// <summary>
        /// 
        /// </summary>
        private static TraceSource _appTracing;

        /// <summary>
        /// Gets the app tracing.
        /// </summary>
        public static TraceSource AppTracing
        {
            get
            {
                return _appTracing;
            }
        }

        /// <summary>
        /// The exceptions
        /// </summary>
        private static Stack<Exception> _exceptions = new Stack<Exception>();

        /// <summary>
        /// Registers the trace source.
        /// </summary>
        /// <param name="source">The source.</param>
        public static void RegisterTraceSource(
            TraceSource source)
        {
            _appTracing = source;
        }
        #endregion

        #region Exception management

        /// <summary>
        /// Sets the exception.
        /// </summary>
        /// <param name="e">The e.</param>
        public static void SetException(Exception e)
        {
            if (e == null)
                return;

            lock (_exceptions)
            {
                _exceptions.Push(e);
            }
        }
        #endregion

        #region Error
        /// <summary>
        /// Traces the Error.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The error.</param>
        public static void TraceError(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Error, 
                text);
        }

        /// <summary>
        /// Traces the error.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceError(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceError(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the error.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The error.</param>
        /// <param name="e">The e.</param>
        public static void TraceError(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceError(
                source, 
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e));
        }
        #endregion

        #region Critical
        /// <summary>
        /// Traces the Critical.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Critical.</param>
        public static void TraceCritical(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Critical,
                text);
        }

        /// <summary>
        /// Traces the critical.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceCritical(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceCritical(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the Critical.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Critical.</param>
        /// <param name="e">The e.</param>
        public static void TraceCritical(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceCritical(
                source,
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e.ToString()));
        }
        #endregion

        #region Critical_Throw
        /// <summary>
        /// Traces the Critical_Throw.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Critical_Throw.</param>
        public static void TraceCritical_Throw(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Critical,
                text);

            ApplicationException exception = new ApplicationException(text);
            exception.Data.Add(Keywords.ExceptionSource, Keywords.TraceCritical_Throw);

            SetException(exception);

            throw exception;
        }

        /// <summary>
        /// Traces the critical.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceCritical_Throw(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceCritical_Throw(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the Critical_Throw.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Critical_Throw.</param>
        /// <param name="e">The e.</param>
        public static void TraceCritical_Throw(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceCritical(
                source,
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e.ToString()));

            throw e;
        }
        #endregion

        #region Warning
        /// <summary>
        /// Traces the Warning.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Warning.</param>
        public static void TraceWarning(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Warning,
                text);
        }

        /// <summary>
        /// Traces the warning.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceWarning(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceWarning(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the Warning.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Warning.</param>
        /// <param name="e">The e.</param>
        public static void TraceWarning(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceWarning(
                source,
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e));
        }
        #endregion

        #region Info
        /// <summary>
        /// Traces the Info.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Info.</param>
        public static void TraceInfo(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Information,
                text);
        }

        /// <summary>
        /// Traces the warning.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceInfo(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceInfo(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the Info.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Info.</param>
        /// <param name="e">The e.</param>
        public static void TraceInfo(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceInfo(
                source,
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e));
        }
        #endregion

        #region MainTrace
        /// <summary>
        /// Traces the event direct.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="type">The type.</param>
        /// <param name="text">The text.</param>
        public static void TraceEventDirect(
            this TraceSource source,
            TraceEventType type,
            string text)
        {
            try
            {
                if (EventTraced != null)
                {
                    EventTraced(
                        source,
                        new TraceEventArgs()
                        {
                            EventDate = DateTime.UtcNow,
                            EventType = type,
                            EventText = text,
                            Exceptions = _exceptions.ToList()
                        });
                }
            }
            finally
            {
                if (source != null)
                {
                    source.TraceEvent(
                        type,
                        0,
                        text);
                }
            }
        }
        #endregion

        #region Verbose
        /// <summary>
        /// Traces the Verbose.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Verbose.</param>
        public static void TraceVerbose(
            this TraceSource source,
            string text)
        {
            TraceEventDirect(
                source,
                TraceEventType.Verbose,
                text);
        }

        /// <summary>
        /// Traces the warning.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="format">The format.</param>
        /// <param name="args">The args.</param>
        public static void TraceVerbose(
            this TraceSource source,
            string format,
            params object[] args)
        {
            TraceVerbose(
                source,
                string.Format(
                    format,
                    args));
        }

        /// <summary>
        /// Traces the Verbose.
        /// </summary>
        /// <param name="source">The source.</param>
        /// <param name="text">The Verbose.</param>
        /// <param name="e">The e.</param>
        public static void TraceVerbose(
            this TraceSource source,
            string text,
            Exception e)
        {
            SetException(e);

            TraceVerbose(
                source,
                string.Format(
                    "{0}{1}{2}",
                    text,
                    Environment.NewLine,
                    e));
        }
        #endregion
    }
}
