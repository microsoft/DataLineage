Onboarding is easy with just a few configurations in Synapse Spark Pool environment and taking code scripts from SparkLin Branch.
Overall Steps looks like:

• Upload the Jar “openlineage-spark:.jar” into the Synapse Spark Pool Packages.

•	Add spark configurations related to open lineage in Synapse Spark Pool.

•	Create a new storage account with a blob container name openlineage to have all json files uploaded and 2 azure table storage (EventMetadata and LineageDetails)

•	Create the Azure Function App and add functions related to SparkLin.

•	Create Purview Collection where all lineage assets will reside.

**Cluster Setup**

OpenLineage integrates with Spark by implementing SparkListener (SparkListenerSQLExecution, SparkListenerEvent) interface and collecting information about jobs that are executed inside a Spark application.

To activate the listener, add the following properties to your Spark configuration: 

•	spark.extraListeners	io.openlineage.spark.agent.OpenLineageSparkListener

Once the listener is activated, it needs to know where to report lineage events, as well as the namespace of your jobs. Add the following additional configuration lines to your Spark Configuration in the Spark pool.

•	spark.openlineage.host                 {your.openlineage.host i.e. func app endpoint url}

•	spark.openlineage.namespace            {your.openlineage.namespace}

•	spark.openlineage.url.param.code       {your func app host key}

•	spark.openlineage.version              { 1 or v1 depends on the jar}



**Storage Account Setup**
Create new storage account in Azure portal by using portal wizard.
    •	create new container and name it as **openlineage**


**Azure Table Storages setup**
We work with 2 azure table storages, one to store all events and their processing status and other is to store all lineage details.

**Creation**
Open your sotrage account and go to **Tables** and create two new blank tables and name them as EventMetadata and LineageDetails.

<img width="248" alt="image" src="https://user-images.githubusercontent.com/123259339/214266628-8ce0ccc7-0811-481e-bc5d-ef97b7cf992a.png">


EventMetadata table is used to store all events information which is triggered by open lineage and to track parsing status of each event.
Structure of EventMetadata table looks like below after rows generated:
1. PartitionKey
2. RowKey
3. Timestamp
4. Status
5. RetryCount
6. FilePath
7. isArchived
8. Message

LineageDetails table is used to store all lineage information which is obtained after json parsing.
Structure of LineageDetails table looks like:
1. PartitionKey
2. RowKey
3. Timestamp
4. derived_columns
5. input_columns
6. input_tables
7. isdelta
8. isglobal
9. isintermediate
10. joinconditions
11. output_columns
12. output_table
 
After we have both azure storage tables, we make use of HTTP and Blob Storage based Azure functions to process all open lineage produced jsons.

**Function Apps Set up **

HTTP Trigger Function App:
Create new function app on azure portal with application insights enabled which will provide a http endpoint for spark cluster to make PUSH requests with json type data. This is C# based function app.

Deployment:
Deploy function from URL: https://github.com/microsoft/DataLineage/tree/main/sparklin/OpenLineage

Add new Configurations in Function App:

• ConnectionString   :   < your new storage account connection string >
• ContainerName      :   openlineage
• TableName          :   EventMetadata

**What does function do**
1. App will store this json data as file into blob storage
2. App will insert an entry in eventmetadata table with status as Unprocessed for this particular json file

Blob Trigger Function App:
Create new function app on azure portal with application insights enabled which will get tiggered as and when new blobs will be uploaded by http trigger function app. This is python based function app.

Deployment:
Deploy function from URL: https://github.com/microsoft/DataLineage/tree/main/sparklin/BlobTriggerFuncApp

Add new Configurations in Function App:

datalineagesynapsestrpoc_STORAGE   :   < your new storage account connection string >
StorageTableName                   :   LineageDetails
TableName                          :   EventMetadata

**What does function do**
1. App will query EventMetadata table and take all records which are in status of 'Unprocessed'
2. For every event, App will read json file from blob storage and start parsing it using Python code and push all lineage details to ''LineageDetails' Azure table
3. Finally App will update status of working event as 'Processed' if lineage is pushed and 'Failed' if something fails and update 'Message' column with exception details.

**Note: Limitations**
Based on our usecases we have implemented the parser, if we find any new edge usecases, we need to enhance the Parser.
Currently it supports Spark 3.1, for greater Spark versions Parser enhancement is required.
