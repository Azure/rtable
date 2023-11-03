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

    /// <summary>
    /// InitDynamicReplicatedTableEntity is the same as DynamicReplicatedTableEntity except KeyNotFoundException is caught in ReadEntity().
    /// InitDynamicReplicatedTableEntity is used instead of DynamicReplicatedTableEntity when ConvertXStoreTableMode = true. This 
    /// is used to aid in transitioning an existing Azure Table to ReplicatedTable
    /// </summary>
    public class InitDynamicReplicatedTableEntity : DynamicReplicatedTableEntity
    {
        public InitDynamicReplicatedTableEntity()
            : base()
        {
        }

        public InitDynamicReplicatedTableEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
        }

        public InitDynamicReplicatedTableEntity(string partitionKey, string rowKey, string etag, IDictionary<string, object> properties)
            : base(partitionKey, rowKey, etag, properties)
        {
        }

        public InitDynamicReplicatedTableEntity(TableEntity entity)
            : base(entity)
        {
        }

        public override void ReadEntity(IDictionary<string, object> properties)
        {
            Dictionary<string, object> prop = new Dictionary<string, object>(properties);

            // Read ReplicatedTable meta data
            try
            {
                _rtable_RowLock = (bool)prop["_rtable_RowLock"]; prop.Remove("_rtable_RowLock");
            }
            catch (KeyNotFoundException)
            {                          
            }
            try
            {
                _rtable_Version = (long)prop["_rtable_Version"]; prop.Remove("_rtable_Version");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                _rtable_Tombstone = (bool)prop["_rtable_Tombstone"]; prop.Remove("_rtable_Tombstone");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                _rtable_ViewId = (long)prop["_rtable_ViewId"]; prop.Remove("_rtable_ViewId");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                _rtable_Operation = (string)prop["_rtable_Operation"]; prop.Remove("_rtable_Operation");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                _rtable_BatchId = (Guid)prop["_rtable_BatchId"]; prop.Remove("_rtable_BatchId");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                _rtable_LockAcquisition = (DateTimeOffset)prop["_rtable_LockAcquisition"]; prop.Remove("_rtable_LockAcquisition");
            }
            catch (KeyNotFoundException)
            {
            }
            Properties = prop;
        }
    }

    public class DynamicReplicatedTableEntity : ReplicatedTableEntity
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicReplicatedTableEntity"/> class.
        /// </summary>
        public DynamicReplicatedTableEntity() 
            : base()
        {
            this.Properties = new Dictionary<string, object>();
            this._rtable_Operation = ReplicatedTable.GetTableOperation(TableOperationType.Insert);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicTableEntity"/> class with the specified partition key and row key.
        /// </summary>
        /// <param name="partitionKey">The partition key value for the entity.</param>
        /// <param name="rowKey">The row key value for the entity.</param>
        public DynamicReplicatedTableEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, null /* timestamp */, new Dictionary<string, object>())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicTableEntity"/> class with the entity's partition key, row key, ETag (if available/required), and properties.
        /// </summary>
        /// <param name="partitionKey">The entity's partition key.</param>
        /// <param name="rowKey">The entity's row key.</param>
        /// <param name="etag">The entity's current ETag.</param>
        /// <param name="properties">The entity's properties, indexed by property name.</param>
        public DynamicReplicatedTableEntity(string partitionKey, string rowKey, string etag, IDictionary<string, object> properties)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, etag, properties)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicTableEntity"/> class with the entity's partition key, row key, time stamp, ETag (if available/required), and properties.
        /// </summary>
        /// <param name="partitionKey">The entity's partition key.</param>
        /// <param name="rowKey">The entity's row key.</param>
        /// <param name="timestamp">The timestamp for this entity as returned by Windows Azure.</param>
        /// <param name="etag">The entity's current ETag; set to null to ignore the ETag during subsequent update operations.</param>
        /// <param name="properties">An <see cref="IDictionary{TKey,TElement}"/> containing a map of <see cref="string"/> property names to <see cref="object"/> data typed values to store in the new <see cref="DynamicTableEntity"/>.</param>
        internal DynamicReplicatedTableEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag, IDictionary<string, object> properties)
            : this()
        {
            if ((partitionKey == null) || (rowKey == null) || (properties == null))
            {
                throw new Exception("The value for firstName cannot be null.");
            }


            // Store the information about this entity.  Make a copy of
            // the properties list, in case the caller decides to reuse
            // the list.
            this.PartitionKey = partitionKey;
            this.RowKey = rowKey;
            this.Timestamp = timestamp;
            this.ETag = new ETag(etag);

            this.Properties = properties;
        }

        public DynamicReplicatedTableEntity(TableEntity entity)
            : this()
        {
            if (entity == null)
            {
                throw new ArgumentNullException(nameof(entity));
            }


            // Store the information about this entity.  Make a copy of
            // the properties list, in case the caller decides to reuse
            // the list.
            this.PartitionKey = entity.PartitionKey;
            this.RowKey = entity.RowKey;
            this.Timestamp = entity.Timestamp;
            this.ETag = entity.ETag;

            foreach (var key in entity.Keys)
            {
                this.Properties[key] = entity[key];
            }

            ReadEntity(Properties);
        }

#if !WINDOWS_RT
        /// <summary>
        /// Gets or sets the entity's property, given the name of the property.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The property.</returns>
        public object this[string key]
        {
            get { return this.Properties[key]; }
            set { this.Properties[key] = value; }
        }
#endif

        /// <summary>
        /// Deserializes this <see cref="DynamicTableEntity"/> instance using the specified <see cref="Dictionary{TKey,TValue}"/> of property names to values of type <see cref="object"/>.
        /// </summary>
        /// <param name="properties">A collection containing the <see cref="Dictionary{TKey,TValue}"/> of string property names mapped to values of type <see cref="object"/> to store in this <see cref="DynamicTableEntity"/> instance.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object used to track the execution of the operation.</param>
        public override void ReadEntity(IDictionary<string, object> properties)
        {
            Dictionary<string, object> prop = new Dictionary<string, object>(properties);

            // Read ReplicatedTable meta data
            _rtable_RowLock = (bool)prop["_rtable_RowLock"]; prop.Remove("_rtable_RowLock");
            _rtable_Version = (long)prop["_rtable_Version"]; prop.Remove("_rtable_Version");
            _rtable_Tombstone = (bool)prop["_rtable_Tombstone"]; prop.Remove("_rtable_Tombstone");
            _rtable_ViewId = (long)prop["_rtable_ViewId"]; prop.Remove("_rtable_ViewId");
            _rtable_Operation = (string)prop["_rtable_Operation"]; prop.Remove("_rtable_Operation");
            _rtable_BatchId = (Guid)prop["_rtable_BatchId"]; prop.Remove("_rtable_BatchId");
            _rtable_LockAcquisition = (DateTimeOffset)prop["_rtable_LockAcquisition"]; prop.Remove("_rtable_LockAcquisition");
            prop.Remove("PartitionKey");
            prop.Remove("RowKey");
            prop.Remove("Timestamp");
            prop.Remove("odata.etag");
            Properties = prop;
        }

        /// <summary>
        /// Serializes the <see cref="Dictionary{TKey,TValue}"/> of property names mapped to values of type <see cref="object"/> from this <see cref="DynamicTableEntity"/> instance.
        /// </summary>
        /// <returns>A collection containing the map of string property names to values of type <see cref="object"/> stored in this <see cref="DynamicTableEntity"/> instance.</returns>
        public override IDictionary<string, object> WriteEntity()
        {
            // Write ReplicatedTable meta data
            Dictionary<string, object> prop = new Dictionary<string, object>(Properties);

            prop["_rtable_RowLock"] = (bool?)_rtable_RowLock;
            prop["_rtable_Version"] = (long?)_rtable_Version;
            prop["_rtable_Tombstone"] = (bool?)_rtable_Tombstone;
            prop["_rtable_ViewId"] = (long?)_rtable_ViewId;
            prop["_rtable_Operation"] = (string)_rtable_Operation;
            prop["_rtable_BatchId"] = (Guid?)_rtable_BatchId;
            prop["_rtable_LockAcquisition"] = (DateTimeOffset)_rtable_LockAcquisition;

            return prop;
        }
    }
}