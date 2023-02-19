using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;

namespace OpenLineage
{
    class TableStorage
    {
        string connectionString = Environment.GetEnvironmentVariable("ConnectionString");
        string tableName = Environment.GetEnvironmentVariable("TableName");
        CloudTable table = null;

        public TableStorage()
        {
            createTableIfNotExist();
        }

        public async void createTableIfNotExist()
        {
            CloudStorageAccount storageAcc = CloudStorageAccount.Parse(connectionString);
            CloudTableClient tblclient = storageAcc.CreateCloudTableClient();
            table = tblclient.GetTableReference(tableName);
            await table.CreateIfNotExistsAsync();
 
        }

        public async void insetEventMetadata(EventMetadata eventMetadata)
        {
            if(table==null) createTableIfNotExist();
            TableOperation insertOperation = TableOperation.Insert(eventMetadata);
            await table.ExecuteAsync(insertOperation);
        }
    }
}
