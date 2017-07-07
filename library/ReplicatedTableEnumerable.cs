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
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Implement an IEnumerable(T)
    /// Wrapps an IEnumerable(T) object
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ReplicatedTableEnumerable<T> : IEnumerable<T>
    {
        private readonly IEnumerable<T> _collection;
        private readonly bool isConvertMode;
        private readonly long txnViewId;
        private readonly StaleViewHandling staleViewHandling;

        public ReplicatedTableEnumerable(IEnumerable<T> collection, bool isConvertMode, long txnViewId, StaleViewHandling staleViewHandling)
        {
            _collection = collection;
            this.isConvertMode = isConvertMode;
            this.txnViewId = txnViewId;
            this.staleViewHandling = staleViewHandling;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return new ReplicatedTableEnumerator<T>(_collection.GetEnumerator(), isConvertMode, txnViewId, staleViewHandling);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}