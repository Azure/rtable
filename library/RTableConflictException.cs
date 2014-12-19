//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.WindowsAzure.Storage.RTable
{
    using System;

    public class RTableConflictException : StorageException
    {
        public RTableConflictException()
        {
        }

        public RTableConflictException(string message)
            : base(message)
        {
        }

        public RTableConflictException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
