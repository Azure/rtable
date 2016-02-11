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
    using System.Linq;
    using System.Diagnostics;
    using System.Diagnostics.Tracing;

    internal class ReplicatedTableEventListener : EventListener
    {
        private TraceSource traceSource = new TraceSource(Constants.LogSourceName);

        internal ReplicatedTableEventListener()
        {
        }

        public override void Dispose()
        {
            try
            {
                if (this.traceSource != null)
                {
                    this.traceSource.Close();
                    this.traceSource = null;
                }

                base.Dispose();
            }
            catch (Exception)
            {
            }
        }

        protected override void OnEventWritten(EventWrittenEventArgs eventData)
        {
            string msg = "";
            if (eventData.Payload != null && eventData.Payload.Any())
            {
                msg = (string)eventData.Payload.Aggregate((current, e) => string.Format("{0}, {1}", current, e));
            }

            this.traceSource.TraceEvent(
                                GetEventTypeFromEventLevel(eventData.Level),
                                ReplicatedTableLogger.EventIdOffSet + (eventData.EventId),
                                string.Format("[{0}][{1}] {2}", ReplicatedTableLogger.LogPrefix, DateTime.Now, msg)
                                );

            this.traceSource.Flush();
        }


        private static TraceEventType GetEventTypeFromEventLevel(EventLevel eventLevel)
        {
            switch (eventLevel)
            {
                case EventLevel.Error:
                    return TraceEventType.Error;

                case EventLevel.Warning:
                    return TraceEventType.Warning;

                case EventLevel.Informational:
                    return TraceEventType.Information;
            }

            return TraceEventType.Verbose;
        }
    }
}
