// The MIT License (MIT)
//
// Copyright (c) 2015 Microsoft Corporation
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN

namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Json;
    using System.Text;

    class JsonStore<T> where T : class
    {
        /// <summary>
        /// JSON Serialization
        /// </summary>
        public static string Serialize(T t)
        {
            string jsonString = string.Empty;
            MemoryStream memoryStream = new MemoryStream();

            try
            {
                DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(T));
                serializer.WriteObject(memoryStream, t);
                jsonString = Encoding.UTF8.GetString(memoryStream.ToArray());
            }
            catch (Exception e)
            {
                string message = String.Format(CultureInfo.InvariantCulture, "Could not perform Json Serialization on {0}", t);
                throw new SerializationException(message, e);
            }
            finally
            {
                memoryStream.Close();
            }

            jsonString = ConvertDoubleQuotesToSingleQuoteForXmlCompatibility(jsonString);

            // JSon converts a / forward slash into a \/ on serialization. That cannot be prevented. We need to remove it since svdgenerator does not work.
            jsonString = ConvertEscapedForwardSlashToForwardSlashForSvdGeneratorCompatibility(jsonString);

            return jsonString;
        }

        private static string ConvertEscapedForwardSlashToForwardSlashForSvdGeneratorCompatibility(string jsonString)
        {
            return jsonString.Replace("\\/", "/");
        }

        private static string ConvertDoubleQuotesToSingleQuoteForXmlCompatibility(string jsonString)
        {
            if (jsonString.Contains("'"))
            {
                string message = String.Format(CultureInfo.InvariantCulture, "Could not perform Json Serialization on {0} since it contains ' single-quote", jsonString);
                throw new SerializationException(message);
            }

            return jsonString.Replace("\"", "'");
        }

        /// <summary>
        /// JSON Deserialization
        /// </summary>
        public static T Deserialize(string jsonString)
        {
            jsonString = ConvertSingleQuotesToDoubleQuotesForJsonDeserialization(jsonString);
            jsonString = ConvertForwardSlashToEscapedForwardSlashForJsonDeserialization(jsonString);

            T t = default(T);
            MemoryStream memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString));

            try
            {
                DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(T));
                t = serializer.ReadObject(memoryStream) as T;
            }
            catch (Exception e)
            {
                string message = String.Format(CultureInfo.InvariantCulture, "Could not perform Json Deserialization on {0}", jsonString);
                throw new SerializationException(message, e);
            }
            finally
            {
                memoryStream.Close();
            }

            return t;
        }

        private static string ConvertForwardSlashToEscapedForwardSlashForJsonDeserialization(string jsonString)
        {
            return jsonString.Replace("/", "\\/");
        }

        private static string ConvertSingleQuotesToDoubleQuotesForJsonDeserialization(string jsonString)
        {
            return jsonString.Replace("'", "\"");
        }
    }
}
