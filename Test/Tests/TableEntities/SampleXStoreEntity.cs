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


namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using global::Azure;
    using global::Azure.Data.Tables;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Exactly the same as SampleRTableEntity, except this class is an "XStore Table Entity"
    /// </summary>
    public sealed class SampleXStoreEntity : ITableEntity
    {
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

        /// <summary>
        /// PartitionKey = JobType.ToLower().Replace(" ", "") 
        /// </summary>
        public string JobType { get; set; }

        /// <summary>
        /// RowKey = JobId.ToLower().Replace(" ", "");
        /// </summary>
        public string JobId { get; set; }

        /// <summary>
        /// This column will store some string value. Change the value of this column in the unit test.
        /// </summary>
        public string Message { get; set; }

        public SampleXStoreEntity()
        {
        }

        public SampleXStoreEntity(string jobType, string jobId, string message)
        {
            string partitionKey;
            string rowKey;
            if (string.IsNullOrEmpty(jobType))
            {
                jobType = "EmptyJobType";
            }
            if (string.IsNullOrEmpty(jobId))
            {
                jobId = "EmptyJobId";
            }

            GenerateKeys(jobType, jobId, out partitionKey, out rowKey);

            PartitionKey = partitionKey;
            RowKey = rowKey;

            JobType = jobType;
            JobId = jobId;
            Message = message;
        }

        /// <summary>
        /// Copy the values of JobType, JobId, Message to the specified object
        /// </summary>
        /// <param name="dst"></param>
        public void CopyTo(SampleRTableEntity dst)
        {
            dst.JobType = this.JobType;
            dst.JobId = this.JobId;
            dst.Message = this.Message;
        }

        /// <summary>
        /// Check whether this instance matches the specified object or not
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            SampleXStoreEntity dst = obj as SampleXStoreEntity;
            if (dst == null)
            {
                return false;
            }
            else
            {
                return (this.JobType == dst.JobType && this.JobId == dst.JobId && this.Message == dst.Message);
            }
        }


        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return string.Format("\tJobType={0}\n\tJobId={1}\n\tMessage={2}\n\tETag={3}",
                JobType,
                JobId,
                Message,
                ETag);
        }

        /// <summary>
        /// Helper function to find out the PartitionKey and RowKey
        /// </summary>
        /// <param name="jobType"></param>
        /// <param name="jobId"></param>
        /// <param name="partitionKey"></param>
        /// <param name="rowKey"></param>
        public static void GenerateKeys(string jobType, string jobId, out string partitionKey, out string rowKey)
        {
            partitionKey = jobType.ToLower().Replace(" ", "");
            rowKey = jobId.ToLower().Replace(" ", "");
        }

        /// <summary>
        /// Helper function to generate a timestamp and a random Guid. Caller can set "Message" to this random string for testing.
        /// </summary>
        /// <returns></returns>
        public static string GenerateRandomMessage()
        {
            return string.Format("{0:MM/dd/yyyy HH:mm:ss.fff} {1}", DateTime.UtcNow, Guid.NewGuid());
        }

        public static SampleXStoreEntity ToSampleXStoreEntity(DynamicReplicatedTableEntity dynamicReplicatedTableEntity)
        {
            var entity = new SampleXStoreEntity()
            {
                PartitionKey = dynamicReplicatedTableEntity.PartitionKey,
                RowKey = dynamicReplicatedTableEntity.RowKey,
                Timestamp = dynamicReplicatedTableEntity.Timestamp,
                ETag = dynamicReplicatedTableEntity.ETag
            };

            // we could reflect, but keeping it simple.
            entity.JobType = (string)dynamicReplicatedTableEntity.Properties["JobType"];
            entity.JobId = (string)dynamicReplicatedTableEntity.Properties["JobId"];
            entity.Message = (string)dynamicReplicatedTableEntity.Properties["Message"];

            return entity;
        }

        public static InitDynamicReplicatedTableEntity ToInitDynamicReplicatedTableEntity(SampleXStoreEntity xstoreEntity)
        {
            IDictionary<string, object> properties = new Dictionary<string, object>();

            // we could reflect, but keeping it simple.
            properties.Add("JobType", xstoreEntity.JobType);
            properties.Add("JobId", xstoreEntity.JobId);
            properties.Add("Message", xstoreEntity.Message);

            InitDynamicReplicatedTableEntity entity = new InitDynamicReplicatedTableEntity(
                                                                xstoreEntity.PartitionKey,
                                                                xstoreEntity.RowKey,
                                                                xstoreEntity.ETag.ToString(),
                                                                properties);
            return entity;
        }
    }
}
