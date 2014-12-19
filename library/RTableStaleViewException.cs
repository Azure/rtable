//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.WindowsAzure.Storage.RTable
{
    using System;

    public class RTableStaleViewException : StorageException
    {
        public RTableStaleViewException()
        {
        }

        public RTableStaleViewException(string message)
            : base(message)
        {
        }

        public RTableStaleViewException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
