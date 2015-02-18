//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{
    using Microsoft.WindowsAzure.Storage.Table;

    internal class POCOEntity : TableEntity
    {
        public string test { get; set; }
        public string a { get; set; }
        public string b { get; set; }
        public string c { get; set; }
    }
}
