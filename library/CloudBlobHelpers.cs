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
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Blob;

    public enum ReadBlobResult
    {
        NotFound = 0, // Must start at 0
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
        BlobsNotInSyncOrTransitioning,
    }

    public enum QuorumWriteResult
    {
        Conflict_UpdateInProgress,
        ReadFailure_Exceptions,
        ReadFailure_NoQuorum,
        Conflict_BlobHasChanged,
        ReadFailure_BlobsNotInSyncOrTransitioning,
        Success,
        QuorumWriteFailure,
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

        public static List<ReadBlobResult> TryReadAllBlobs<T>(List<CloudBlockBlob> blobs, out List<T> values, out List<string> eTags, Func<string, T> ParseBlobFunc)
                    where T : class
        {
            int numberOfBlobs = blobs.Count;

            T[] valuesArray = new T[numberOfBlobs];
            string[] eTagsArray = new string[numberOfBlobs];
            ReadBlobResult[] resultArray = new ReadBlobResult[numberOfBlobs];

            // read from all the blobs in parallel
            Parallel.For(0, numberOfBlobs, index =>
            {
                T currentValue;
                string currentETag;

                resultArray[index] = TryReadBlob(blobs[index], out currentValue, out currentETag, ParseBlobFunc);
                valuesArray[index] = currentValue;
                eTagsArray[index] = currentETag;
            });

            values = valuesArray.ToList();
            eTags = eTagsArray.ToList();

            return resultArray.ToList();
        }

        public static QuorumReadResult TryReadBlobQuorum<T>(List<CloudBlockBlob> blobs, out T value, out List<string> eTags, Func<string, T> ParseBlobFunc)
            where T : class
        {
            value = default(T);
            eTags = null;

            // Fetch all blobs ...
            List<T> valuesArray;
            List<string> eTagsArray;
            List<ReadBlobResult> resultArray = TryReadAllBlobs(blobs, out valuesArray, out eTagsArray, ParseBlobFunc);

            // Find majority ...
            int quorumIndex;
            QuorumReadResult majority = FindMajority(resultArray, valuesArray, out quorumIndex);

            if (majority == QuorumReadResult.Success)
            {
                value = valuesArray[quorumIndex];
                eTags = eTagsArray;
            }

            return majority;
        }

        private static QuorumReadResult FindMajority<T>(List<ReadBlobResult> resultArray, List<T> valuesArray, out int quorumIndex)
            where T : class
        {
            quorumIndex = -1;

            int numberOfBlobs = resultArray.Count;
            int quorum = (numberOfBlobs / 2) + 1;

            int[] counters = new int[Enum.GetNames(typeof(ReadBlobResult)).Length];
            foreach (var result in resultArray)
            {
                counters[(int)result]++;
            }

            // - NotFound
            if (counters[(int)ReadBlobResult.NotFound] >= quorum)
            {
                return QuorumReadResult.NotFound;
            }

            // - UpdateInProgress
            if (counters[(int)ReadBlobResult.UpdateInProgress] >= quorum)
            {
                return QuorumReadResult.UpdateInProgress;
            }

            // - Either StorageException or Exception
            if (counters[(int)ReadBlobResult.StorageException] + counters[(int)ReadBlobResult.Exception] >= quorum)
            {
                return QuorumReadResult.Exception;
            }

            // - 0 <= Sucess < Quorum
            if (counters[(int)ReadBlobResult.Success] < quorum)
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
                    quorumIndex = index;
                    return QuorumReadResult.Success;
                }
            }

            // - Quorum not posible
            return QuorumReadResult.BlobsNotInSyncOrTransitioning;
        }

        public static QuorumWriteResult TryWriteBlobQuorum<T>(List<CloudBlockBlob> blobs, T configuration, Func<string, T> ParseBlobFunc, Func<T, T, bool> ConfigIdComparer, Func<T, T> GenerateConfigId)
            where T : class
        {
            // Fetch all blobs ...
            List<T> valuesArray;
            List<string> eTagsArray;
            List<ReadBlobResult> resultArray = TryReadAllBlobs(blobs, out valuesArray, out eTagsArray, ParseBlobFunc);

            // Find majority ...
            int quorumIndex;
            QuorumReadResult majority = FindMajority(resultArray, valuesArray, out quorumIndex);

            switch (majority)
            {
                case QuorumReadResult.NotFound:
                    // Create blobs ...
                    break;

                case QuorumReadResult.UpdateInProgress:
                    return QuorumWriteResult.Conflict_UpdateInProgress;

                case QuorumReadResult.Exception:
                    return QuorumWriteResult.ReadFailure_Exceptions;

                case QuorumReadResult.NullOrLowSuccessRate:
                    return QuorumWriteResult.ReadFailure_NoQuorum;

                case QuorumReadResult.Success:
                    // Blob has changed since ...
                    if (ConfigIdComparer(valuesArray[quorumIndex], configuration) == false)
                    {
                        return QuorumWriteResult.Conflict_BlobHasChanged;
                    }

                    // Update blobs ...
                    break;

                case QuorumReadResult.BlobsNotInSyncOrTransitioning:
                    return QuorumWriteResult.ReadFailure_BlobsNotInSyncOrTransitioning;

                default:
                {
                    var msg = string.Format("Unexpected value majority=\'{0}\' ", majority);
                    throw new Exception(msg);
                }
            }

            // Generate a new Id for the copy of the input configuration
            T newConfiguration = GenerateConfigId(configuration);
            string content = newConfiguration.ToString();

            // Update blobs ...
            int numberOfBlobs = blobs.Count;
            bool[] writeResultArray = new bool[numberOfBlobs];

            Parallel.For(0, numberOfBlobs, index =>
            {
                T currentValue = valuesArray[index];
                string currentETag = eTagsArray[index];

                if (currentValue == null)
                {
                    currentETag = null;
                }
                else if (!ConfigIdComparer(currentValue, configuration))
                {
                    currentETag = "*";
                }

                writeResultArray[index] = TryWriteBlob(blobs[index], content, currentETag);
            });

            int successRate = writeResultArray.Count(e => e == true);
            if (successRate == numberOfBlobs)
            {
                return QuorumWriteResult.Success;
            }

            return QuorumWriteResult.QuorumWriteFailure;
        }

        public static bool TryWriteBlob(CloudBlockBlob blob, string content, string eTag)
        {
            try
            {
                AccessCondition condition = string.IsNullOrEmpty(eTag)
                                                ? AccessCondition.GenerateEmptyCondition()
                                                : AccessCondition.GenerateIfMatchCondition(eTag);

                blob.UploadText(content, accessCondition: condition);

                return true;
            }
            catch (StorageException e)
            {
                ReplicatedTableLogger.LogError("Updating the blob: {0} failed. Exception: {1}", blob, e.Message);
            }

            return false;
        }

        public static void TryWriteBlob(CloudBlockBlob blob, string content)
        {
            try
            {
                //Step 1: Delete the current configuration
                blob.UploadText(Constants.ConfigurationStoreUpdatingText);

                //Step 2: Wait for L + CF to make sure no pending transaction working on old views
                Thread.Sleep(TimeSpan.FromSeconds(Constants.LeaseDurationInSec + Constants.ClockFactorInSec));

                //Step 3: Update new config
                blob.UploadText(content);
            }
            catch (StorageException e)
            {
                ReplicatedTableLogger.LogError("Updating the blob: {0} failed. Exception: {1}", blob, e.Message);
            }
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
