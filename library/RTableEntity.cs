//-----------------------------------------------------------------------
// <copyright file="RTableEntity.cs" company="Microsoft">
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
//-

namespace Microsoft.WindowsAzure.Storage.RTable
{
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    //this is the class that users extend to store their own data in a row
    public class RTableEntity : IRTableEntity
    {

        /// Gets or sets the properties in the table entity, indexed by property name.
        /// </summary>
        /// <value>The entity properties.</value>
        public IDictionary<string, EntityProperty> Properties { get; set; }

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
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the entity's current ETag. Set this value to '*' to blindly overwrite an entity as part of an update operation.
        /// </summary>
        /// <value>The entity ETag.</value>
        public string ETag { get; set; }

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

        public RTableEntity()
        {
            this._rtable_LockAcquisition = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.FromTicks(0));
        }
        
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TableEntity"/> class with the specified partition key and row key.
        /// </summary>
        /// <param name="partitionKey">The partition key of the <see cref="TableEntity"/> to be initialized.</param>
        /// <param name="rowKey">The row key of the <see cref="TableEntity"/> to be initialized.</param>
        public RTableEntity(string partitionKey, string rowKey)
            : this()
        {
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
        }

        /// <summary>
        /// Deserializes this <see cref="DynamicTableEntity"/> instance using the specified <see cref="Dictionary{TKey,TValue}"/> of property names to values of type <see cref="EntityProperty"/>.
        /// </summary>
        /// <param name="properties">A collection containing the <see cref="Dictionary{TKey,TValue}"/> of string property names mapped to values of type <see cref="EntityProperty"/> to store in this <see cref="DynamicTableEntity"/> instance.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object used to track the execution of the operation.</param>
        public virtual void  ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            TableEntity.ReadUserObject(this, properties, operationContext);
        }

        /// <summary>
        /// Serializes the <see cref="Dictionary{TKey,TValue}"/> of property names mapped to values of type <see cref="EntityProperty"/> from this <see cref="DynamicTableEntity"/> instance.
        /// </summary>
        /// <param name="operationContext">An <see cref="OperationContext"/> object used to track the execution of the operation.</param>
        /// <returns>A collection containing the map of string property names to values of type <see cref="EntityProperty"/> stored in this <see cref="DynamicTableEntity"/> instance.</returns>
        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return TableEntity.WriteUserObject(this, operationContext);
        }

        public override bool Equals(object obj)
        {
            RTableEntity that = obj as RTableEntity;
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
