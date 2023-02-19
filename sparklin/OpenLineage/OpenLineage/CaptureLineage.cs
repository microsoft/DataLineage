using System;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System.Linq;

namespace OpenLineage
{
    public static class CaptureLineage
    {
        [FunctionName("CaptureLineage")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "1/lineage")] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string eventType = req.Query["eventType"];

            string[] predefined_class_list = {"org.apache.spark.sql.execution.datasources.CreateTable",
                "org.apache.spark.sql.catalyst.plans.logical.CreateViewStatement",
                "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement",
                "org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement",
                "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand",
            "org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable"};

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            eventType = eventType ?? data?.eventType;
            string className = data["run"]["facets"]["spark.logicalPlan"]["plan"][0]["class"];
            string runId = data["run"]["runId"];
            string notebookName = data["job"]["name"];
            notebookName = notebookName.Substring(0,notebookName.IndexOf(".")-1);

            if (eventType != null && eventType.Equals("COMPLETE")  &&  predefined_class_list.Contains(className))
            {
                string connectionString = Environment.GetEnvironmentVariable("ConnectionString");
                string containerName = Environment.GetEnvironmentVariable("ContainerName");
                TableStorage tableStorage = new TableStorage();

                string currentTimestamp = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
                string fileName = $"{runId}_{notebookName}_{currentTimestamp}.json";
                EventMetadata eventMetadata = new EventMetadata(Utility.getQualifierName(notebookName), $"{runId}_{notebookName}_{currentTimestamp}");
                eventMetadata.Status = Constant.UN_PROCESSED;
                eventMetadata.RetryCount = Constant.RETRY_COUNT;
                eventMetadata.isArchived = Constant.IS_ARCHIVE;
                eventMetadata.FilePath = $"{containerName}/{fileName}";

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudBlobClient client = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer container = client.GetContainerReference(containerName);
                
                CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
                blob.Properties.ContentType = "application/json";
                using (Stream stream = new MemoryStream(Encoding.UTF8.GetBytes(requestBody)))
                {
                    await blob.UploadFromStreamAsync(stream);
                }
                tableStorage.insetEventMetadata(eventMetadata);
                return new OkObjectResult("file uploaded successfylly");
            }
            else
            {
                return new OkObjectResult("Event Type is not COMPLETE Or ClassName Not Matched");
            }
        }
    }

    
}
