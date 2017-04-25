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
    using System.Diagnostics.Tracing;

    public class ReplicatedTableEtwLogger : IReplicatedLogger
    {
        private static ReplicatedTableEventSource eventSource = new ReplicatedTableEventSource();
        private static ReplicatedTableEventListener eventListener = new ReplicatedTableEventListener();

        public static void EnableEvents(EventLevel level = EventLevel.Verbose, int eventIdOffSet = 0, string logPrefix = "")
        {
            ReplicatedTableEtwLogger.eventListener.EventIdOffSet = eventIdOffSet;
            ReplicatedTableEtwLogger.eventListener.LogPrefix = logPrefix;
            ReplicatedTableEtwLogger.eventListener.EnableEvents(ReplicatedTableEtwLogger.eventSource, level);
        }

        public static void DisableEvents()
        {
            ReplicatedTableEtwLogger.eventListener.DisableEvents(eventSource);
        }

        public static void DisposeLogger()
        {
            ReplicatedTableEtwLogger.eventListener.Dispose();
            ReplicatedTableEtwLogger.eventSource.Dispose();
        }


        public void LogMessage(EventLevel eventLevel, string format, params object[] args)
        {
            if (ReplicatedTableEtwLogger.eventSource.IsEnabled())
            {
                switch (eventLevel)
                {
                    case EventLevel.Error:
                        ReplicatedTableEtwLogger.eventSource.Error(string.Format(format, args));
                        break;

                    case EventLevel.Warning:
                        ReplicatedTableEtwLogger.eventSource.Warning(string.Format(format, args));
                        break;

                    case EventLevel.Informational:
                        ReplicatedTableEtwLogger.eventSource.Informational(string.Format(format, args));
                        break;

                    case EventLevel.Verbose:
                        ReplicatedTableEtwLogger.eventSource.Verbose(string.Format(format, args));
                        break;

                    // ...
                }
            }
        }
    }
}
