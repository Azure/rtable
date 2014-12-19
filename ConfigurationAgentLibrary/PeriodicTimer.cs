namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    
    /// <summary>
    /// http://msdn.microsoft.com/en-us/library/ah1h85ch.aspx for instantiating Timer Class
    /// </summary>
    public class PeriodicTimer
    {
        private Timer timer;

        public TimerCallback Callback { get; private set; }
        public Object State { get; private set; }
        public TimeSpan DueTime { get; private set; }
        public TimeSpan Period { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="PeriodicTimer" /> class.
        /// </summary>
        /// <param name="callback">The callback method.</param>
        /// <param name="period">Periodicity of the task.</param>
        public PeriodicTimer(TimerCallback callback, TimeSpan period) : this(callback, null, TimeSpan.Zero, period) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="PeriodicTimer" /> class.
        /// </summary>
        /// <param name="callback">The callback method.</param>
        /// <param name="state">Object state to pass to callback</param>
        /// <param name="period">Periodicity of the task.</param>
        public PeriodicTimer(TimerCallback callback, Object state, TimeSpan period) : this(callback, state, TimeSpan.Zero, period) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="PeriodicTimer" /> class.
        /// </summary>
        /// <param name="callback">The callback method.</param>
        /// <param name="state">Object state to pass to callback</param>
        /// <param name="dueTime">Due time in milliseconds</param>
        /// <param name="period">Periodicity of the task.</param>
        public PeriodicTimer(TimerCallback callback, Object state, TimeSpan dueTime, TimeSpan period)
        {
            Callback = callback;
            State = state;
            DueTime = dueTime;
            Period = period;
            timer = new Timer(InternalCallback, State, DueTime, TimeSpan.Zero);
        }

        public void InternalCallback(object state)
        {
            Callback(State);
            timer.Change(Period, TimeSpan.Zero);
        }

        public void Stop()
        {
            timer.Change(Timeout.Infinite, Timeout.Infinite);
            timer.Dispose();
        }

        public void Start()
        {
            timer.Change(Period, TimeSpan.Zero);
        }
    }
}

