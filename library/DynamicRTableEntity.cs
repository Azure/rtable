using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Core.Util;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System.Configuration;

namespace Microsoft.WindowsAzure.Storage.RTable
{
    /// <summary>
    /// DynamicRTableEntity2 is the same as DynamicRTableEntity except KeyNotFoundException is caught in ReadEntity().
    /// DynamicRTableEntity2 is used instead of DynamicRTableEntity when ConvertXStoreTableMode = true.
    /// </summary>
    public class DynamicRTableEntity2 : DynamicRTableEntity
    {
        public DynamicRTableEntity2()
            : base()
        {
        }

        public DynamicRTableEntity2(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Dictionary<string, EntityProperty> prop = new Dictionary<string, EntityProperty>(properties);

            // Read RTable meta data
            try
            {
                RowLock = (bool)prop["RowLock"].BooleanValue; prop.Remove("RowLock");
            }
            catch (KeyNotFoundException)
            {                          
            }
            try
            {
                Version = (long)prop["Version"].Int64Value; prop.Remove("Version");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                Tombstone = (bool)prop["Tombstone"].BooleanValue; prop.Remove("Tombstone");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                ViewId = (long)prop["ViewId"].Int64Value; prop.Remove("ViewId");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                Operation = (string)prop["Operation"].StringValue; prop.Remove("Operation");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                BatchId = (Guid)prop["BatchId"].GuidValue; prop.Remove("BatchId");
            }
            catch (KeyNotFoundException)
            {
            }
            try
            {
                LockAcquisition = (DateTimeOffset)prop["LockAcquisition"].DateTimeOffsetValue; prop.Remove("LockAcquisition");
            }
            catch (KeyNotFoundException)
            {
            }
            Properties = prop;
        }
    }

    public class DynamicRTableEntity : RTableEntity
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicRTableEntity"/> class.
        /// </summary>
        public DynamicRTableEntity() 
            : base()
        {
            this.Properties = new Dictionary<string, EntityProperty>();
            this.Operation = RTable.GetTableOperation(TableOperationType.Insert);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicTableEntity"/> class with the specified partition key and row key.
        /// </summary>
        /// <param name="partitionKey">The partition key value for the entity.</param>
        /// <param name="rowKey">The row key value for the entity.</param>
        public DynamicRTableEntity(string partitionKey, string rowKey)
            : this(partitionKey, rowKey, DateTimeOffset.MinValue, null /* timestamp */, new Dictionary<string, EntityProperty>())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DynamicTableEntity"/> class with the entity's partition key, row key, ETag (if available/required), and properties.
        /// </summary>
        /// <param name="partitionKey">The entity's partition key.</param>
        /// <param name="rowKey">The entity's row key.</param>
        /// <param name="etag">The entity's current ETag.</param>
        /// <param name="properties">The entity's properties, indexed by property name.</param>
        public DynamicRTableEntity(string partitionKey, string rowKey, string etag, IDictionary<string, EntityProperty> properties)
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
        /// <param name="properties">An <see cref="IDictionary{TKey,TElement}"/> containing a map of <see cref="string"/> property names to <see cref="EntityProperty"/> data typed values to store in the new <see cref="DynamicTableEntity"/>.</param>
        internal DynamicRTableEntity(string partitionKey, string rowKey, DateTimeOffset timestamp, string etag, IDictionary<string, EntityProperty> properties)
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
            this.ETag = etag;

            this.Properties = properties;
        }

#if !WINDOWS_RT
        /// <summary>
        /// Gets or sets the entity's property, given the name of the property.
        /// </summary>
        /// <param name="key">The name of the property.</param>
        /// <returns>The property.</returns>
        public EntityProperty this[string key]
        {
            get { return this.Properties[key]; }
            set { this.Properties[key] = value; }
        }
#endif

        /// <summary>
        /// Deserializes this <see cref="DynamicTableEntity"/> instance using the specified <see cref="Dictionary{TKey,TValue}"/> of property names to values of type <see cref="EntityProperty"/>.
        /// </summary>
        /// <param name="properties">A collection containing the <see cref="Dictionary{TKey,TValue}"/> of string property names mapped to values of type <see cref="EntityProperty"/> to store in this <see cref="DynamicTableEntity"/> instance.</param>
        /// <param name="operationContext">An <see cref="OperationContext"/> object used to track the execution of the operation.</param>
        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            Dictionary<string, EntityProperty> prop = new Dictionary<string, EntityProperty>(properties);

            // Read RTable meta data
            RowLock = (bool)prop["RowLock"].BooleanValue; prop.Remove("RowLock");
            Version = (long)prop["Version"].Int64Value; prop.Remove("Version");
            Tombstone = (bool)prop["Tombstone"].BooleanValue; prop.Remove("Tombstone");
            ViewId = (long)prop["ViewId"].Int64Value; prop.Remove("ViewId");
            Operation = (string)prop["Operation"].StringValue; prop.Remove("Operation");
            BatchId = (Guid)prop["BatchId"].GuidValue; prop.Remove("BatchId");
            LockAcquisition = (DateTimeOffset)prop["LockAcquisition"].DateTimeOffsetValue; prop.Remove("LockAcquisition");
            Properties = prop;
        }

        /// <summary>
        /// Serializes the <see cref="Dictionary{TKey,TValue}"/> of property names mapped to values of type <see cref="EntityProperty"/> from this <see cref="DynamicTableEntity"/> instance.
        /// </summary>
        /// <param name="operationContext">An <see cref="OperationContext"/> object used to track the execution of the operation.</param>
        /// <returns>A collection containing the map of string property names to values of type <see cref="EntityProperty"/> stored in this <see cref="DynamicTableEntity"/> instance.</returns>
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            // Write RTable meta data
            Dictionary<string, EntityProperty> prop = new Dictionary<string, EntityProperty>(Properties);

            prop.Add("RowLock", new EntityProperty((bool?)RowLock));
            prop.Add("Version", new EntityProperty((long?)Version));
            prop.Add("Tombstone", new EntityProperty((bool?)Tombstone));
            prop.Add("ViewId", new EntityProperty((long?)ViewId));
            prop.Add("Operation", new EntityProperty((string)Operation));
            prop.Add("BatchId", new EntityProperty((Guid?)BatchId));
            prop.Add("LockAcquisition", new EntityProperty((DateTimeOffset)LockAcquisition));

            return prop;
        }
    }
}