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
    using global::Azure;
    using global::Azure.Data.Tables;
    using System;
    using System.Collections.Generic;
    using System.Reflection;

    //this is the class that users extend to store their own data in a row
    public class ReplicatedTableEntity : IReplicatedTableEntity
    {

        /// Gets or sets the properties in the table entity, indexed by property name.
        /// </summary>
        /// <value>The entity properties.</value>
        public IDictionary<string, object> Properties { get; set; }

        /// <summary>
        /// Gets or sets the entity's partition key.
        /// </summary>
        /// <value>The entity partition key.</value>
        public string PartitionKey { get; set; }

        /// <summary>
        /// Gets or sets the entity's row key.
        /// </summary>
        /// <value>The entity row key.</value>
        public string RowKey { get; set; }

        /// <summary>
        /// Gets or sets the entity's timestamp.
        /// </summary>
        /// <value>The entity timestamp.</value>
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the entity's current ETag. Set this value to '*' to blindly overwrite an entity as part of an update operation.
        /// </summary>
        /// <value>The entity ETag.</value>
        public ETag ETag { get; set; }

        // lock bit: used to detect that replication is in progress
        public bool _rtable_RowLock { get; set; }

        // version: used as (virtual) etag for replicated table with different (physical) etags  
        public long _rtable_Version { get; set; }

        // tomb stone: to support deletes
        public bool _rtable_Tombstone { get; set; }

        // view number: to support changes in replica configurations
        public long _rtable_ViewId { get; set; }
        
        // _rtable_Operation type: last operation type that modified this data
        public string _rtable_Operation { get; set; } 

        // Batch id: to support batch operations
        public Guid _rtable_BatchId { get; set; }

        /// <summary>
        /// If _rtable_RowLock=true, it is the timestamp (UTC) in which the _rtable_RowLock could be reset back to false.
        /// </summary>
        public DateTimeOffset _rtable_LockAcquisition { get; set; }

        public ReplicatedTableEntity()
        {
            this._rtable_LockAcquisition = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.FromTicks(0));
        }
        
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TableEntity"/> class with the specified partition key and row key.
        /// </summary>
        /// <param name="partitionKey">The partition key of the <see cref="TableEntity"/> to be initialized.</param>
        /// <param name="rowKey">The row key of the <see cref="TableEntity"/> to be initialized.</param>
        public ReplicatedTableEntity(string partitionKey, string rowKey)
            : this()
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        /// <summary>
        /// Deserializes this <see cref="DynamicTableEntity"/> instance using the specified <see cref="Dictionary{TKey,TValue}"/> of property names to values of type <see cref="object"/>.
        /// </summary>
        /// <param name="properties">A collection containing the <see cref="Dictionary{TKey,TValue}"/> of string property names mapped to values of type <see cref="object"/> to store in this <see cref="DynamicTableEntity"/> instance.</param>
        public virtual void ReadEntity(IDictionary<string, object> properties)
        {
            foreach (PropertyInfo allProperty in this.GetType().GetTypeInfo().GetAllProperties())
            {
                if (ShouldSkipProperty(allProperty))
                {
                    continue;
                }

                if (!properties.ContainsKey(allProperty.Name))
                {
                    ReplicatedTableLogger.LogInformational("Omitting property '{0}' from de-serialization because there is no corresponding entry in the dictionary provided.", allProperty.Name);
                    continue;
                }

                allProperty.SetValue(this, properties[allProperty.Name]);
            }
        }

        /// <summary>
        /// Serializes the <see cref="Dictionary{TKey,TValue}"/> of property names mapped to values of type <see cref="object"/> from this <see cref="DynamicTableEntity"/> instance.
        /// </summary>
        /// <returns>A collection containing the map of string property names to values of type <see cref="object"/> stored in this <see cref="DynamicTableEntity"/> instance.</returns>
        public virtual IDictionary<string, object> WriteEntity()
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            foreach (PropertyInfo allProperty in this.GetType().GetTypeInfo().GetAllProperties())
            {
                if (!ShouldSkipProperty(allProperty))
                {
                    var obj = allProperty.GetValue(this, null);
                    if (obj != null)
                    {
                        dictionary.Add(allProperty.Name, obj);
                    }
                }
            }

            return dictionary;
        }

        internal static bool ShouldSkipProperty(PropertyInfo property)
        {
            switch (property.Name)
            {
                case "PartitionKey":
                case "RowKey":
                case "Timestamp":
                case "ETag":
                    return true;
                default:
                    {
                        MethodInfo methodInfo = property.SetMethod;
                        MethodInfo methodInfo2 = property.SetMethod;
                        if ((object)methodInfo == null || !methodInfo.IsPublic || (object)methodInfo2 == null || !methodInfo2.IsPublic)
                        {
                            ReplicatedTableLogger.LogInformational("Omitting property '{0}' from serialization/de-serialization because the property's getter/setter are not public.", property.Name);
                            return true;
                        }

                        if (methodInfo.IsStatic)
                        {
                            return true;
                        }

                        return false;
                    }
            }
        }

        public override bool Equals(object obj)
        {
            ReplicatedTableEntity that = obj as ReplicatedTableEntity;
            if(that == null)
                return false;

            if ((this.PartitionKey != that.PartitionKey) ||
                (this.RowKey != that.RowKey) ||
                (this._rtable_RowLock != that._rtable_RowLock) ||
                (this._rtable_Version != that._rtable_Version) ||
                (this._rtable_Tombstone != that._rtable_Tombstone))
            {
                return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
