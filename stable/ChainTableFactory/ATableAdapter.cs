using Microsoft.WindowsAzure.Storage.ChainTableInterface;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.ChainTableFactory
{
    class ATableAdapter : IChainTable
    {
        // This Adapter adapts a normal Azure Table (ATable) to the IComposableTable interface.

        public ATableAdapter(CloudTable table)
        {
            this.table = table;
        }

        public Table.TableResult Execute(Table.TableOperation operation, Table.TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            var res = table.Execute(operation, requestOptions, operationContext);
            return res;
        }

        public IList<Table.TableResult> ExecuteBatch(Table.TableBatchOperation batch, Table.TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            try
            {
                return table.ExecuteBatch(batch, requestOptions, operationContext);
            }
            catch (StorageException e)
            {
                // find the error index
                // WARNING: this is not very reliable
                // c.f. http://stackoverflow.com/questions/14282385/azure-cloudtable-executebatchtablebatchoperation-throws-a-storageexception-ho/14290910#14290910
                var msg = e.RequestInformation.ExtendedErrorInformation.ErrorMessage;
                var parts = msg.Split(':');
                var errorIndex = int.Parse(parts[0]);

                throw new ChainTableInterface.ChainTableBatchException(errorIndex, e);
            }
        }

        public Task<Table.TableResult> ExecuteAsync(Table.TableOperation operation, Table.TableRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            return table.ExecuteAsync(operation, requestOptions, operationContext);
        }



        private CloudTable table;


        public string GetTableID()
        {
            return table.Name;
        }


        public bool CreateIfNotExists()
        {
            return table.CreateIfNotExists();
        }

        public bool DeleteIfExists()
        {
            return table.DeleteIfExists();
        }


        public IEnumerable<T> ExecuteQuery<T>(TableQuery<T> q) where T : ITableEntity, new()
        {
            return table.ExecuteQuery<T>(q);
        }
    }
}
