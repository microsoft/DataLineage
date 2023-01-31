SparkLin is a Custom Parser which parses the Spark Internal Logical Execution Plan and fetches the required Attributes, entities and the Transformations/functions applied on Columns and Join Conditions on Entities.
SparkLin uses OpenLineage 0.4 Version Jar which is tightly coupled with Spark 3.1 Version and provides more detailed plan.
This Project uses two Function Apps which are used for Capturing the Event Json Payloads from OpenLineage into Blob Storage and read or parse the Jsons automatically . 
We are constantly improving the SparkLin to tackle different usecases where the Synapse Notebooks contain Complete Spark Native Code.
