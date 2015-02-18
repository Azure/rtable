//////////////////////////////////////////////////////////////////////////////
//
// Copyright (c) Microsoft Corporation. All rights reserved.
//
//////////////////////////////////////////////////////////////////////////////

namespace Microsoft.Azure.Toolkit.Replication.Test
{    
    using Microsoft.Azure.Toolkit.Replication;
    using NUnit.Framework;

    internal class InternalEntity : ReplicatedTableEntity
    {
        public InternalEntity()
        {
        }

        public InternalEntity(string pk, string rk)
            : base(pk, rk)
        {
        }

        public void Populate()
        {
            this.Foo = "bar";
            this.A = "a";
            this.B = "b";
            this.C = "c";
            this.D = "d";
        }

        public string Foo { get; set; }
        public string A { get; set; }
        public string B { get; set; }
        public string C { get; set; }
        public string D { get; set; }

        public void Validate()
        {
            Assert.AreEqual("bar", this.Foo, "this.Foo={0} != bar", this.Foo);
            Assert.AreEqual("a", this.A, "this.A={0} != a", this.A);
            Assert.AreEqual("b", this.B, "this.B={0} != b", this.B);
            Assert.AreEqual("c", this.C, "this.C={0} != c", this.C);
            Assert.AreEqual("d", this.D, "this.FooD{0} != d", this.D);
        }
    }
}
