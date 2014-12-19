namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Json;
    using System.Text;

    public class JsonStore<T> where T : class
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
