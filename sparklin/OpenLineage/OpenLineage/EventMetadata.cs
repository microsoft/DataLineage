using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.WindowsAzure.Storage.Table;

namespace OpenLineage
{
    class EventMetadata : TableEntity
    {
        public EventMetadata(String teamName, String fileName)
        {
            PartitionKey = teamName;
            RowKey = fileName;
        }

        public EventMetadata() { }
       
        public string Status { get; set; }
        public string Message { get; set; }
        public int RetryCount { get; set; }
        public String FilePath { get; set; }

        public Boolean isArchived { get; set; }

}
}
