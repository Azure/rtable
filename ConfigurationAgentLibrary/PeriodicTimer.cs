//-----------------------------------------------------------------------
// <copyright file="PeriodicTimer.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Threading;
    
    /// <summary>
    /// http://msdn.microsoft.com/en-us/library/ah1h85ch.aspx for instantiating Timer Class
    /// </summary>
    class PeriodicTimer
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

