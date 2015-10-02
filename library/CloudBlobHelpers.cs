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
    using System.Security;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
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

        public static ReplicatedTableReadBlobResult TryReadBlob<T>(CloudBlockBlob blob, out T configuration, out string eTag, Func<string, T> ParseBlobFunc)
            where T : class
        {
            configuration = default(T);
            eTag = null;

            try
            {
                string content = blob.DownloadText();
                if (content == Constants.ConfigurationStoreUpdatingText)
                {
                    return new ReplicatedTableReadBlobResult(ReadBlobCode.UpdateInProgress, "Blob update in progress ...");
                }

                configuration = ParseBlobFunc(content);
                eTag = blob.Properties.ETag;

                return new ReplicatedTableReadBlobResult(ReadBlobCode.Success, "");
            }
            catch (StorageException e)
            {
                var msg = string.Format("Error reading blob: {0}. StorageException: {1}", blob.Uri, e.Message);
                ReplicatedTableLogger.LogError(msg);

                if (e.RequestInformation != null &&
                    e.RequestInformation.HttpStatusCode == (int)HttpStatusCode.NotFound)
                {
                    return new ReplicatedTableReadBlobResult(ReadBlobCode.NotFound, msg);
                }

                return new ReplicatedTableReadBlobResult(ReadBlobCode.StorageException, msg);
            }
            catch (Exception e)
            {
                var msg = string.Format("Error reading blob: {0}. Exception: {1}", blob.Uri, e.Message);
                ReplicatedTableLogger.LogError(msg);

                return new ReplicatedTableReadBlobResult(ReadBlobCode.Exception, msg);
            }
        }

        public static List<ReplicatedTableReadBlobResult> TryReadAllBlobs<T>(List<CloudBlockBlob> blobs, out List<T> values, out List<string> eTags, Func<string, T> ParseBlobFunc)
                    where T : class
        {
            int numberOfBlobs = blobs.Count;

            T[] valuesArray = new T[numberOfBlobs];
            string[] eTagsArray = new string[numberOfBlobs];
            ReplicatedTableReadBlobResult[] resultArray = new ReplicatedTableReadBlobResult[numberOfBlobs];

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

        public static ReplicatedTableQuorumReadResult TryReadBlobQuorum<T>(List<CloudBlockBlob> blobs, out T value, out List<string> eTags, Func<string, T> ParseBlobFunc)
            where T : class
        {
            value = default(T);
            eTags = null;

            // Fetch all blobs ...
            List<T> valuesArray;
            List<string> eTagsArray;
            List<ReplicatedTableReadBlobResult> resultArray = TryReadAllBlobs(blobs, out valuesArray, out eTagsArray, ParseBlobFunc);

            // Find majority ...
            int quorumIndex;
            ReplicatedTableQuorumReadResult majority = FindMajority(resultArray, valuesArray, out quorumIndex);

            if (majority.Code == ReplicatedTableQuorumReadCode.Success)
            {
                value = valuesArray[quorumIndex];
                eTags = eTagsArray;
            }

            return majority;
        }

        private static ReplicatedTableQuorumReadResult FindMajority<T>(List<ReplicatedTableReadBlobResult> resultArray, List<T> valuesArray, out int quorumIndex)
            where T : class
        {
            quorumIndex = -1;

            int numberOfBlobs = resultArray.Count;
            int quorum = (numberOfBlobs / 2) + 1;

            int[] counters = new int[Enum.GetNames(typeof(ReadBlobCode)).Length];
            foreach (var result in resultArray)
            {
                counters[(int)result.Code]++;
            }

            // - NotFound
            if (counters[(int)ReadBlobCode.NotFound] >= quorum)
            {
                return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.NotFound, resultArray);
            }

            // - UpdateInProgress
            if (counters[(int)ReadBlobCode.UpdateInProgress] >= quorum)
            {
                return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.UpdateInProgress, resultArray);
            }

            // - Either StorageException or Exception
            if (counters[(int)ReadBlobCode.StorageException] + counters[(int)ReadBlobCode.Exception] >= quorum)
            {
                return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.Exception, resultArray);
            }

            // - 0 <= Sucess < Quorum
            if (counters[(int)ReadBlobCode.Success] < quorum)
            {
                return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.NullOrLowSuccessRate, resultArray);
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
                    return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.Success, resultArray);
                }
            }

            // - Quorum not posible
            return new ReplicatedTableQuorumReadResult(ReplicatedTableQuorumReadCode.BlobsNotInSyncOrTransitioning, resultArray);
        }

        public static ReplicatedTableQuorumWriteResult TryWriteBlobQuorum<T>(List<CloudBlockBlob> blobs, T configuration, Func<string, T> ParseBlobFunc, Func<T, T, bool> ConfigIdComparer, Func<T, T> GenerateConfigId)
            where T : ReplicatedTableConfigurationBase, new()
        {
            // Fetch all blobs ...
            List<T> valuesArray;
            List<string> eTagsArray;
            List<ReplicatedTableReadBlobResult> resultArray = TryReadAllBlobs(blobs, out valuesArray, out eTagsArray, ParseBlobFunc);

            // Find majority ...
            int quorumIndex;
            ReplicatedTableQuorumReadResult majority = FindMajority(resultArray, valuesArray, out quorumIndex);
            string readDetails = majority.ToString();

            switch (majority.Code)
            {
                case ReplicatedTableQuorumReadCode.NotFound:
                    // Create blobs ...
                    break;

                case ReplicatedTableQuorumReadCode.UpdateInProgress:
                    return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.ConflictDueToUpdateInProgress, readDetails);

                case ReplicatedTableQuorumReadCode.Exception:
                    return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.ReadExceptions, readDetails);

                case ReplicatedTableQuorumReadCode.NullOrLowSuccessRate:
                    return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.ReadNoQuorum, readDetails);

                case ReplicatedTableQuorumReadCode.Success:
                    // Blob has changed since ...
                    if (ConfigIdComparer(valuesArray[quorumIndex], configuration) == false)
                    {
                        return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.ConflictDueToBlobChange, readDetails);
                    }

                    // Update blobs ...
                    break;

                case ReplicatedTableQuorumReadCode.BlobsNotInSyncOrTransitioning:
                    return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.BlobsNotInSyncOrTransitioning, readDetails);

                default:
                {
                    var msg = string.Format("Unexpected value majority=\'{0}\' ", majority.Code);
                    throw new Exception(msg);
                }
            }

            // Generate a new Id for the copy of the input configuration
            T newConfiguration = GenerateConfigId(configuration);
            string content = newConfiguration.ToString();

            // Update blobs ...
            int numberOfBlobs = blobs.Count;
            ReplicatedTableWriteBlobResult[] writeResultArray = new ReplicatedTableWriteBlobResult[numberOfBlobs];

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

            int successRate = writeResultArray.Count(e => e.Success == true);
            int quorum = (numberOfBlobs / 2) + 1;

            if (successRate >= quorum)
            {
                // Return new config Id to the caller for record
                string newConfId = newConfiguration.GetConfigId().ToString();
                return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.Success, newConfId, writeResultArray.ToList());
            }

            return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.QuorumWriteFailure, writeResultArray.ToList());
        }

        public static ReplicatedTableQuorumWriteResult TryUploadBlobs<T>(List<CloudBlockBlob> blobs, T configuration)
            where T : class
        {
            string content = configuration.ToString();

            // Update blobs ...
            int numberOfBlobs = blobs.Count;
            ReplicatedTableWriteBlobResult[] writeResultArray = new ReplicatedTableWriteBlobResult[numberOfBlobs];

            Parallel.For(0, numberOfBlobs, index =>
            {
                writeResultArray[index] = TryWriteBlob(blobs[index], content, "*");
            });

            int successRate = writeResultArray.Count(e => e.Success == true);
            if (successRate == numberOfBlobs)
            {
                return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.Success, writeResultArray.ToList());
            }

            return new ReplicatedTableQuorumWriteResult(ReplicatedTableQuorumWriteCode.QuorumWriteFailure, writeResultArray.ToList());
        }

        public static ReplicatedTableWriteBlobResult TryWriteBlob(CloudBlockBlob blob, string content, string eTag)
        {
            try
            {
                AccessCondition condition = string.IsNullOrEmpty(eTag)
                                                ? AccessCondition.GenerateEmptyCondition()
                                                : AccessCondition.GenerateIfMatchCondition(eTag);

                blob.UploadText(content, accessCondition: condition);

                return new ReplicatedTableWriteBlobResult(true, "");
            }
            catch (StorageException e)
            {
                var msg = string.Format("Updating the blob: {0} failed. Exception: {1}", blob, e.Message);

                ReplicatedTableLogger.LogError(msg);
                return new ReplicatedTableWriteBlobResult(false, msg);
            }
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

        public static bool TryCreateCloudTableClient(SecureString connectionString, out CloudTableClient cloudTableClient)
        {
            cloudTableClient = null;

            try
            {
                string decryptConnectionString = SecureStringHelper.ToString(connectionString);
                cloudTableClient = CloudStorageAccount.Parse(decryptConnectionString).CreateCloudTableClient();

                return true;
            }
            catch (Exception e)
            {
                ReplicatedTableLogger.LogError(
                    "Error creating cloud table client: Connection string {0}. Exception: {1}",
                    "********",
                    e.Message);
            }

            return false;
        }
    }
}
