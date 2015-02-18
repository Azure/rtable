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
