import logging
import os
import datetime as dt
import json
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient, ContentSettings 
from .tablestorage import tablestorage
from .event import event


def uploadblob(json_in, blobname, conn_str, lin_container):

    container_client = ContainerClient.from_connection_string(conn_str, container_name=lin_container)
    blob = BlobClient.from_connection_string(conn_str, container_name= lin_container, blob_name=blobname)
    BlobClient
    blob.upload_blob(json_in, overwrite=True)


def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("http trigger function kicked off")

    lineageContainerStr = os.environ["LINEAGE_STORAGE_CONN_STR"]
    lineageContainer = os.environ["LINEAGE_CONTAINER"]

    data = req.get_json()

    currenttimestamp = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    eventType = data["eventType"]
    className = data["run"]["facets"]["spark.logicalPlan"]["plan"][0]["class"]
    runId = data["run"]["runId"]
    notebookName = data["job"]["name"]
    notebookName = notebookName[0 : notebookName.index('.')]
    
    fileName = runId + '_' + notebookName + '_' + currenttimestamp + '.json'
    filePath = lineageContainer + '/' + fileName

    predefined_class_list = ["org.apache.spark.sql.execution.datasources.CreateTable",
                "org.apache.spark.sql.catalyst.plans.logical.CreateViewStatement",
                "org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelectStatement",
                "org.apache.spark.sql.catalyst.plans.logical.InsertIntoStatement",
                "org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand"]

    if eventType is not None and eventType == "COMPLETE" and className in predefined_class_list :
        # upload file as json into blob storage
        uploadblob(json.dumps(data), fileName, lineageContainerStr, lineageContainer)
        
        #code to add new unprocessed row for same uploaded json into azure table  
        eventrow = event('HRSI', fileName)
        eventrow.Status = 'Unprocessed'
        eventrow.RetryCount = 3
        eventrow.FilepPath = filePath
        eventrow.isArchived = False
        eventrow.Message = ''

        tableStorage = tablestorage()
        tableStorage.insertEventMetadata(eventrow.__dict__)

        return func.HttpResponse(f"Func App successfully processed http request")

    else:

        return func.HttpResponse(f"Event Type is not COMPLETE Or ClassName Not Matched")