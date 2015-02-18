// azure-rtable ver. 0.9
//
// Copyright (c) Microsoft Corporation
//
// All rights reserved. 
//
// MIT License
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files 
// (the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, 
// merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished 
// to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF 
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
// FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION 
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


namespace Microsoft.Azure.Toolkit.Replication
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

