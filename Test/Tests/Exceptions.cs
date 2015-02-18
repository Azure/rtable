namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using System;

    class RTableException : Exception
    {
        public RTableException(string message)
            : base(message)
        {
        }
    }

    class RTableResourceNotFoundException : RTableException
    {
        public RTableResourceNotFoundException(string message)
            : base(message)
        {
        }
    }

    class RTableConflictException : RTableException
    {
        public RTableConflictException(string message)
            : base(message)
        {
        }
    }

    class RTableRetriableException : RTableException
    {
        public RTableRetriableException(string message)
            : base(message)
        {
        }
    }

}
