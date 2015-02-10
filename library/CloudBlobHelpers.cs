// Copyright (c) Microsoft Corporation
// All rights reserved.
//
// The MIT License (MIT)
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
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN



namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Blob;

    class CloudBlobHelpers
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
                if (content == Constants.ConfigurationStoreUpdatingText)
                {
                    return false;
                }

                string blobContent = content;
                configurationStore = JsonStore<T>.Deserialize(blobContent);
                return true;
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Error reading blob: {0}. Exception: {1}", 
                    blob.Uri, 
                    e.Message);
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
                ReplicatedTableLogger.LogError("Error creating cloud table client: Connection string {0}. Exception: {1}", 
                    storageAccountConnectionString, 
                    e.Message);
            }

            return false;
        }
    }
}
