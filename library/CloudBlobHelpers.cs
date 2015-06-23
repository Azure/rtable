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


namespace Microsoft.Azure.Toolkit.Replication
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Blob;

    public enum ReadBlobResult
    {
        NotFound,
        UpdateInProgress,
        StorageException,
        Exception,
        Success,
    }

    public enum QuorumReadResult
    {
        NotFound,
        UpdateInProgress,
        Exception,
        NullOrLowSuccessRate,
        Success,
        QuorumNotPossible,
    }

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

        public static ReadBlobResult TryReadBlob<T>(CloudBlockBlob blob, out T configuration, out string eTag, Func<string, T> ParseBlobFunc)
            where T : class
        {
            configuration = default(T);
            eTag = null;

            try
            {
                string content = blob.DownloadText();
                if (content == Constants.ConfigurationStoreUpdatingText)
                {
                    return ReadBlobResult.UpdateInProgress;
                }

                configuration = ParseBlobFunc(content);
                eTag = blob.Properties.ETag;

                return ReadBlobResult.Success;
            }
            catch (StorageException e)
            {
                ReplicatedTableLogger.LogError("Error reading blob: {0}. StorageException: {1}",
                    blob.Uri,
                    e.Message);

                if (e.RequestInformation != null &&
                    e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return ReadBlobResult.NotFound;
                }

                return ReadBlobResult.StorageException;
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError("Error reading blob: {0}. Exception: {1}", 
                    blob.Uri, 
                    e.Message);
            }

            return ReadBlobResult.Exception;
        }

        public static QuorumReadResult TryReadBlobQuorum<T>(List<CloudBlockBlob> blobs, out T value, out List<string> eTags, Func<string, T> ParseBlobFunc)
            where T : class
        {
            eTags = null;
            value = default(T);

            int numberOfBlobs = blobs.Count;
            int quorum = (numberOfBlobs/2) + 1;

            string[] eTagsArray = new string[numberOfBlobs];
            T[] valuesArray = new T[numberOfBlobs];
            var resultArray = new ReadBlobResult[numberOfBlobs];

            // read from all the blobs in parallel
            Parallel.For(0, numberOfBlobs, index =>
            {
                T currentValue;
                string currentETag;

                resultArray[index] = TryReadBlob(blobs[index], out currentValue, out currentETag, ParseBlobFunc);
                if (resultArray[index] == ReadBlobResult.Success)
                {
                    valuesArray[index] = currentValue;
                    eTagsArray[index] = currentETag;
                }
            });

            /*
             * What majority is saying ?
             */

            // - NotFound
            if (resultArray.Count(res => res == ReadBlobResult.NotFound) >= quorum)
            {
                return QuorumReadResult.NotFound;
            }

            // - UpdateInProgress
            if (resultArray.Count(res => res == ReadBlobResult.UpdateInProgress) >= quorum)
            {
                return QuorumReadResult.UpdateInProgress;
            }

            // - Either StorageException or Exception
            if (resultArray.Count(res => res == ReadBlobResult.StorageException || res == ReadBlobResult.Exception) >= quorum)
            {
                return QuorumReadResult.Exception;
            }

            // - 0 <= Sucess < Quorum
            if (resultArray.Count(res => res == ReadBlobResult.Success) < quorum)
            {
                return QuorumReadResult.NullOrLowSuccessRate;
            }

            // - Find the quorum value
            for (int index = 0; index < quorum; index++)
            {
                if (valuesArray[index] == null)
                {
                    continue;
                }

                int matchCount = 1;

                // optimization to skip over the value if it is the same as the previous one we checked for quorum
                if (index > 0 && valuesArray[index - 1] == valuesArray[index])
                {
                    continue;
                }

                for (int innerLoop = index + 1; innerLoop < numberOfBlobs; innerLoop++)
                {
                    if (valuesArray[index].Equals(valuesArray[innerLoop]))
                    {
                        matchCount++;
                    }
                }

                if (matchCount >= quorum)
                {
                    // we found our quorum value
                    value = valuesArray[index];
                    eTags = eTagsArray.ToList();
                    return QuorumReadResult.Success;
                }
            }

            // - Quorum not posible
            return QuorumReadResult.QuorumNotPossible;
        }

        public static bool TryCreateCloudTableClient(string storageAccountConnectionString, out CloudTableClient cloudTableClient)
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
