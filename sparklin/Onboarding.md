Onboarding is easy with just a few configurations in Synapse Spark Pool environment and taking code scripts from SparkLin Branch.

•	Upload the Jar “openlineage-spark:.jar” into the Synapse Spark Pool Packages.

•	Add spark configurations related to open lineage in Synapse Spark Pool.

•	Create the Azure Function App and add functions related to SparkLin.

•	Create Event Grid Subscription for Blob Storage Account.

•	Create Purview Collection where all lineage assets will reside.

**Cluster Setup**

OpenLineage integrates with Spark by implementing SparkListener (SparkListenerSQLExecution, SparkListenerEvent) interface and collecting information about jobs that are executed inside a Spark application.

To activate the listener, add the following properties to your Spark configuration: 

•	spark.extraListeners	io.openlineage.spark.agent.OpenLineageSparkListener

Once the listener is activated, it needs to know where to report lineage events, as well as the namespace of your jobs. Add the following additional configuration lines to your Spark Configuration in the Spark pool.

•	spark.openlineage.host                        {your.openlineage.host i.e. func app endpoint url}

•	spark.openlineage.namespace            {your.openlineage.namespace}

•	spark.openlineage.url.param.code     {your func app host key}

•	spark.openlineage.version                   { 1 or v1 depends on the jar}

 
