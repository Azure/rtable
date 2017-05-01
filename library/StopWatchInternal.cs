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
    using System.Diagnostics;

    public class StopWatchInternal : IDisposable
    {
        private readonly Stopwatch _stopWatch;
        private readonly string _tableName;
        private readonly string _context;

        private bool _disposed = false;

        public StopWatchInternal(string tableName, string context, IReplicatedTableConfigurationWrapper replicatedTableConfigurationWrapper)
        {
            this._tableName = tableName;
            this._context = context;

            if (replicatedTableConfigurationWrapper.IsIntrumentationEnabled())
            {
                ReplicatedTableLogger.LogVerbose("[Instrumentation] {0}:{1} started", _tableName, _context);

                _stopWatch = Stopwatch.StartNew();
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                if (_stopWatch != null)
                {
                    _stopWatch.Stop();

                    ReplicatedTableLogger.LogVerbose("[Instrumentation] {0}:{1} took {2} ms", _tableName, _context, _stopWatch.ElapsedMilliseconds);
                }
            }

            _disposed = true;
        }
    }
}
