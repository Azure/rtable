// ------------------------------------------------------------------------------------------
// <copyright file="TableOperationType.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation. All rights reserved
// </copyright>
// ------------------------------------------------------------------------------------------

namespace Microsoft.Azure.Toolkit.Replication
{
    public enum TableOperationType
    {
        Insert,
        Delete,
        Replace,
        Merge,
        InsertOrReplace,
        InsertOrMerge,
        Retrieve,
        RotateEncryptionKey
    }
}
