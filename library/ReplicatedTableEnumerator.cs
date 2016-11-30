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
    using System.Collections;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage.Table;


    /// <summary>
    /// Implement an IEnumerator(T)
    /// Wrapps an IEnumerator(T) object
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ReplicatedTableEnumerator<T> : IEnumerator<T>
    {
        private readonly IEnumerator<T> _collection;
        private readonly Action<ITableEntity> VirtualizeEtagFunc;

        public ReplicatedTableEnumerator(IEnumerator<T> collection, Action<ITableEntity> VirtualizeEtagFunc)
        {
            _collection = collection;
            this.VirtualizeEtagFunc = VirtualizeEtagFunc;
        }

        public bool MoveNext() { return _collection.MoveNext(); }
        public void Reset() { _collection.Reset(); }
        void IDisposable.Dispose() { _collection.Dispose(); }
        object IEnumerator.Current { get { return Current; } }

        /// <summary>
        /// Virtualize the ETag of the current entity
        /// </summary>
        public T Current
        {
            get
            {
                T curr = _collection.Current;
                VirtualizeEtagFunc(curr as ITableEntity);
                return curr;
            }
        }
    }
}
