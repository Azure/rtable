// ------------------------------------------------------------------------------------------
// <copyright file="TableClientExtensions.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved
// </copyright>
// ------------------------------------------------------------------------------------------

namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using System.Linq;
    using global::Azure.Data.Tables;

    /// <summary>
    /// Extend TableClient functionality.
    /// </summary>
    public static class TableClientExtensions
    {
        // https://github.com/Azure/azure-sdk-for-net/issues/28392#issuecomment-1109951579
        public static bool Exists(this TableClient tableClient, TableServiceClient tableServiceClient)
        {
            if (tableClient == null)
            {
                throw new ArgumentNullException("tableClient");
            }

            if (tableServiceClient == null)
            {
                throw new ArgumentNullException("tableServiceClient");
            }

            return tableServiceClient.Query(t => t.Name.Equals(tableClient.Name)).Any();
        }
    }
}
