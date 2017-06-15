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
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Implement System.Linq.IQueryable
    /// Wrapps a TableQuery object
    /// </summary>
    /// <typeparam name="TElement"></typeparam>
    public class ReplicatedTableQuery<TElement> : IOrderedQueryable<TElement>
    {
        private readonly IQueryable<TElement> innerTableQuery;
        private readonly bool isConvertMode;
        private readonly long txnViewId;
        private readonly bool throwOnStaleViewFlag;

        internal ReplicatedTableQuery(IQueryable<TElement> innerTableQuery, bool isConvertMode, long txnViewId, bool throwOnStaleViewFlag)
        {
            this.innerTableQuery = innerTableQuery;
            this.isConvertMode = isConvertMode;
            this.txnViewId = txnViewId;
            this.throwOnStaleViewFlag = throwOnStaleViewFlag;

            Provider = new ReplicatedTableQueryProvider(innerTableQuery.Provider, isConvertMode, txnViewId, throwOnStaleViewFlag);
            Expression = innerTableQuery.Expression;
        }

        public IQueryProvider Provider { get; private set; }
        public Expression Expression { get; private set; }
        public Type ElementType { get { return typeof(TElement); } }

        public IEnumerator<TElement> GetEnumerator()
        {
            return new ReplicatedTableEnumerator<TElement>(innerTableQuery.GetEnumerator(), isConvertMode, txnViewId, throwOnStaleViewFlag);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }


    /// <summary>
    /// Implement System.Linq.IQueryProvider
    /// Wrapps an IQueryProvider object
    /// </summary>
    public class ReplicatedTableQueryProvider : IQueryProvider
    {
        private readonly IQueryProvider innerQueryProvider;
        private readonly bool isConvertMode;
        private readonly long txnViewId;
        private readonly bool throwOnStaleViewFlag;

        public ReplicatedTableQueryProvider(IQueryProvider innerQueryProvider, bool isConvertMode, long txnViewId, bool throwOnStaleViewFlag)
        {
            this.innerQueryProvider = innerQueryProvider;
            this.isConvertMode = isConvertMode;
            this.txnViewId = txnViewId;
            this.throwOnStaleViewFlag = throwOnStaleViewFlag;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);

            try
            {
                return (IQueryable)Activator.CreateInstance(
                                                    typeof(ReplicatedTableQuery<>).MakeGenericType(elementType),
                                                    new object[] { innerQueryProvider.CreateQuery(expression), isConvertMode });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        public IQueryable<TResult> CreateQuery<TResult>(Expression expression)
        {
            return new ReplicatedTableQuery<TResult>(innerQueryProvider.CreateQuery<TResult>(expression), isConvertMode, txnViewId, throwOnStaleViewFlag);
        }

        public object Execute(Expression expression)
        {
            return innerQueryProvider.Execute(expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            return innerQueryProvider.Execute<TResult>(expression);
        }
    }


    /// <summary>
    /// Helper class
    /// </summary>
    internal static class TypeSystem
    {
        /// <summary>
        /// Returns the generic type argument of an IEnumerable(T) collection.
        /// </summary>
        /// <param name="seqType"></param>
        /// <returns></returns>
        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null)
            {
                return seqType;
            }
            return ienum.GetGenericArguments()[0];
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            }

            if (seqType.IsGenericType)
            {
                foreach (Type arg in seqType.GetGenericArguments())
                {
                    Type ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            Type[] ifaces = seqType.GetInterfaces();
            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }
    }
}
