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
    using NUnit.Framework;
    using System;
    using System.Linq;
    using Microsoft.WindowsAzure.Storage;

    public class TestHelper
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Anvil.RdUsage!Perf", "27146")]       
        public static void AssertNAttempts(OperationContext ctx, int n)
        {
            Assert.AreEqual(n, ctx.RequestResults.Count(), String.Format("_rtable_Operation took more than {0} attempt(s) to complete", n));
        }

        public static void ValidateResponse(OperationContext opContext, int expectedAttempts, int expectedStatusCode, string[] allowedErrorCodes, string errorMessageBeginsWith)
        {
            ValidateResponse(opContext, expectedAttempts, expectedStatusCode, allowedErrorCodes, new string[] { errorMessageBeginsWith });
        }

        public static void ValidateResponse(OperationContext opContext, int expectedAttempts, int expectedStatusCode, string[] allowedErrorCodes, string[] errorMessageBeginsWith)
        {
            ValidateResponse(opContext, expectedAttempts, expectedStatusCode, allowedErrorCodes, errorMessageBeginsWith, true);
        }

        public static void ValidateResponse(OperationContext opContext, int expectedAttempts, int expectedStatusCode, string[] allowedErrorCodes, string[] errorMessageBeginsWith, bool stripIndex)
        {
            AssertNAttempts(opContext, expectedAttempts);
            Assert.AreEqual(expectedStatusCode, opContext.LastResult.HttpStatusCode, "expectedStatusCode is wrong.");
            Assert.IsTrue(allowedErrorCodes.Contains(opContext.LastResult.ExtendedErrorInformation.ErrorCode), "Unexpected Error Code, received: " + opContext.LastResult.ExtendedErrorInformation.ErrorCode);

            if (errorMessageBeginsWith != null)
            {
                Assert.IsNotNull(opContext.LastResult.ExtendedErrorInformation.ErrorMessage);

                string message = opContext.LastResult.ExtendedErrorInformation.ErrorMessage;
                if (stripIndex)
                {
                    int semDex = opContext.LastResult.ExtendedErrorInformation.ErrorMessage.IndexOf(":");
                    semDex = semDex > 2 ? -1 : semDex;
                    message = message.Substring(semDex + 1);
                }
                Assert.IsTrue(errorMessageBeginsWith.Where((s) => message.StartsWith(s)).Any(), "Got this opContext.LastResult.ExtendedErrorInformation = {0}", opContext.LastResult.ExtendedErrorInformation.ErrorMessage);            
            }
        }

        public static T ExpectedException<T>(Action operation, string operationDescription)
            where T : Exception
        {
            try
            {
                operation();
            }
            catch (T e)
            {
                return e;
            }
            catch (Exception ex)
            {
                T e = ex as T; // Test framework changes the value under debugger
                if (e != null)
                {
                    return e;
                }
                Assert.Fail("Invalid exception {0} for operation: {1}", ex.GetType(), operationDescription);
            }

            Assert.Fail("No exception received while while expecting {0}: {1}", typeof(T).ToString(), operationDescription);
            return null;
        }
    }
}
