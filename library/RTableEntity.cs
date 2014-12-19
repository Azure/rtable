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
        public bool RowLock { get; set; }

        // version: used as (virtual) etag for replicated table with different (physical) etags  
        public long Version { get; set; }

        // tomb stone: to support deletes
        public bool Tombstone { get; set; }

        // view number: to support changes in replica configurations
        public long ViewId { get; set; }
        
        // Operation type: last operation type that modified this data
        public string Operation { get; set; } 

        // Batch id: to support batch operations
        public Guid BatchId { get; set; }

        /// <summary>
        /// If RowLock=true, it is the timestamp (UTC) in which the RowLock could be reset back to false.
        /// </summary>
        public DateTimeOffset LockAcquisition { get; set; }

        public RTableEntity()
        {
            this.LockAcquisition = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.FromTicks(0));
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
                (this.RowLock != that.RowLock) ||
                (this.Version != that.Version) ||
                (this.Tombstone != that.Tombstone))
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
