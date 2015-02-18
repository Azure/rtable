//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.Azure.Toolkit.Replication;

    /// <summary>
    /// Use this wrapper to Read/Write/Update/Delete RTable entities for SampleRTableEntity
    /// </summary>
    public class RTableWrapperForSampleRTableEntity : RTableWrapperBase<SampleRTableEntity>
    {
        /// <summary>
        /// Lock timeout for tests
        /// </summary>
        private const int TestLockTimeoutInSeconds = 5;


        private RTableWrapperForSampleRTableEntity(IReplicatedTable rtable)
            : base(rtable)
        {
        }

        /// <summary>
        /// Get a wrapper to query the specified RTable
        /// </summary>
        /// <param name="rtable"></param>
        public static RTableWrapperForSampleRTableEntity GetRTableWrapper(IReplicatedTable rtable)
        {
            return new RTableWrapperForSampleRTableEntity(rtable);
        }

        protected override void ModifyRowData(SampleRTableEntity tableRow, SampleRTableEntity newRow)
        {
            newRow.CopyTo(tableRow);
        }

        public SampleRTableEntity ReadEntity(string jobType, string jobId)
        {
            string partitionKey;
            string rowKey;
            SampleRTableEntity.GenerateKeys(jobType, jobId, out partitionKey, out rowKey);
            try
            {
                return FindRow(partitionKey, rowKey);
            }
            catch (RTableResourceNotFoundException)
            {
                return null;
            }
        }
    }
}
