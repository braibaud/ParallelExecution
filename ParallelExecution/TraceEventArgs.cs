using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace ParallelExecution
{
    /// <summary>
    /// 
    /// </summary>
    public class TraceEventArgs : EventArgs
    {
        /// <summary>
        /// Gets or sets the event date.
        /// </summary>
        /// <value>
        /// The event date.
        /// </value>
        public DateTime EventDate
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the type of the event.
        /// </summary>
        /// <value>The type of the event.</value>
        public TraceEventType EventType
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the event text.
        /// </summary>
        /// <value>The event text.</value>
        public string EventText
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the exceptions.
        /// </summary>
        /// <value>
        /// The exceptions.
        /// </value>
        public List<Exception> Exceptions
        {
            get;
            set;
        }
    }
}
