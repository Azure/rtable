using System.Runtime.CompilerServices;
using Microsoft.WindowsAzure.Storage.Table;

namespace Microsoft.WindowsAzure.Storage.RTable.ConfigurationAgentLibrary
{
    using System;
    using Microsoft.WindowsAzure.Storage.RTable;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class CloudBlobHelpers
    {
        public static CloudBlockBlob GetBlockBlob(string configurationStorageConnectionString, string configurationLocation)
        {
            CloudStorageAccount blobStorageAccount = CloudStorageAccount.Parse(configurationStorageConnectionString);
            CloudBlobClient blobClient = blobStorageAccount.CreateCloudBlobClient();

            CloudBlobContainer container = blobClient.GetContainerReference(GetContainerName(configurationLocation));
            if (container.CreateIfNotExists())
            {
                container.SetPermissions(new BlobContainerPermissions() { PublicAccess = BlobContainerPublicAccessType.Off });
                container.FetchAttributes(null, new BlobRequestOptions() { ServerTimeout = new TimeSpan(0, 10, 0) });
            }

            return container.GetBlockBlobReference(GetBlobName(configurationLocation));
        }

        /// E.g. in https://accountname.blob.core.windows.net/container/blob.txt
        ///In this example, "container/blob.txt" is the blob path.  The ContainerName is "container" and the BlobName is "blob.txt".
        public static string GetBlobName(string blobPath)
        {
            int firstSlashIndex = blobPath.IndexOf('/');

            string blobName;
            if (firstSlashIndex == blobPath.Length - 1)
            {
                blobName = null;
            }
            else
            {
                blobName = blobPath.Substring(firstSlashIndex + 1);
            }

            return blobName;
        }

        /// E.g. in https://accountname.blob.core.windows.net/container/blob.txt
        ///In this example, "container/blob.txt" is the blob path.  The ContainerName is "container" and the BlobName is "blob.txt".
        public static string GetContainerName(string blobPath)
        {
            int firstSlashIndex = blobPath.IndexOf('/');

            string containerName;
            if (firstSlashIndex == -1)
            {
                containerName = "$root";
            }
            else
            {
                containerName = blobPath.Substring(0, firstSlashIndex);
            }

            return containerName;
        }

        public static bool TryReadBlob<T>(CloudBlockBlob blob, out T configurationStore)
            where T : class
        {
            configurationStore = default(T);
            try
            {
                string content = blob.DownloadText();
                if (content == ConfigurationConstants.ConfigurationStoreUpdatingText)
                {
                    return false;
                }

                string blobContent = content;
                configurationStore = JsonStore<T>.Deserialize(blobContent);
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error reading blob: {0}. Exception: {1}", blob.Uri, e.Message);
            }

            return false;
        }

        public static bool TryCreateCloudTableClient(string storageAccountConnectionString,
            out CloudTableClient cloudTableClient)
        {
            cloudTableClient = null;

            try
            {
                cloudTableClient = CloudStorageAccount.Parse(storageAccountConnectionString).CreateCloudTableClient();
                return true;

            }
            catch (Exception e)
            {
                Console.WriteLine("Error creating cloud table client: Connection string {0}. Exception: {1}", storageAccountConnectionString, e.Message);
            }

            return false;
        }
    }
}
