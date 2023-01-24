import logging

import azure.functions as func
import json
from .Data import AZTableStorage
from azure.data.tables import TableClient
import os, traceback, sys

from .Synapse_JsonParser import PurviewTransform
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient

oauth = ServicePrincipalAuthentication(
    tenant_id=<<tenant_id>>,
    client_id=<<client_id>>,
    client_secret=<<client_secret>>
)

client = PurviewClient(
    account_name=<<purview instance>>,
    authentication=oauth
)

def main(myblob: func.InputStream):

    # logging.info(f"Python blob trigger function processed blob \n"
    #              f"Name: {myblob.name}\n"
    #              f"Blob Size: {myblob.length} bytes")

    fileName = myblob.name.split("/")[1].split(".")[0]
    # logging.info(f"****fileName*******  {fileName} ")
    
    logging.info(f"****TableName*******  {os.getenv('StorageTableName')}")

    # cluster_name, nb_name, input_tables,output_table, _input_cols, _output_cols,deltatable, intermediate_tbl_views,globaltempviews, hardcodecol, joinList = "","","","","","","","","","",""
    
    pt = PurviewTransform(client, json.load(myblob))

    azStorage =  AZTableStorage()

    event_client = azStorage.createClient(os.getenv('TableName'),os.getenv('datalineagesynapsestrpoc_STORAGE'))

    lineage_client = azStorage.createClient(os.getenv('StorageTableName'),os.getenv('datalineagesynapsestrpoc_STORAGE'))

    streamName = "HRSI"

    name_filter = "PartitionKey eq '%s' and RowKey eq '%s' and (Status eq 'Unprocessed' or Status eq 'Parsing Failed')" % (streamName, fileName)
    # # table_client = TableClient.from_connection_string(conn_str=os.getenv('datalineagesynapsestrpoc_STORAGE'), table_name=os.getenv('TableName'))
   
    entities = azStorage.azure_query_entities(event_client, name_filter)
    
    # lineage_metadata = azStorage.create_lineage_entity("HRSI",fileName,"","","","","","","","")
    # event_metadata = azStorage.create_event_entity("HRSI",fileName,"Unprocessed",3,myblob.name,False,"SUCCESS")
    
    # azStorage.insert_entity(event_client, event_metadata)
    # azStorage.insert_entity(lineage_client, lineage_metadata)

    
    # cluster_name, nb_name, input_tables,output_table, _input_cols, _output_cols,deltatable, intermediate_tbl_views,globaltempviews, hardcodecol, joinList = pt.transform_to_purview()
    # lineage_metadata = azStorage.create_lineage_entity(cluster_name, nb_name, input_tables,
    #                                                 output_table, _input_cols, _output_cols,
    #                                                 deltatable, intermediate_tbl_views,
    #                                                 globaltempviews, hardcodecol, joinList)
    # azStorage.azure_upsert_entity(lineage_client, lineage_metadata)

    for entity in entities:
        print('RowKey:' + entity['RowKey'])
        try:
            cluster_name, nb_name, input_tables,output_table, _input_cols, _output_cols,deltatable, intermediate_tbl_views,globaltempviews, hardcodecol, joinList = pt.transform_to_purview()
            if input_tables and output_table:
                lineage_metadata = azStorage.create_lineage_entity(cluster_name, nb_name, input_tables,
                                                            output_table, _input_cols, _output_cols,
                                                            deltatable, intermediate_tbl_views,
                                                            globaltempviews, hardcodecol, joinList)
                azStorage.azure_upsert_entity(lineage_client, lineage_metadata)
            print("Execution Completed")
            metadata = azStorage.create_event_entity("HRSI",fileName,"Processed",3,myblob.name,False,"SUCCESS")
        except BaseException as e:
            print("Exception Caused in Parsing  " + str(e))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            err_msg = traceback.format_exception(exc_type, exc_value,exc_traceback)[-2:]
            # for i in traceback.format_exception(exc_type, exc_value,exc_traceback):
            #     print(i)
            metadata = azStorage.create_event_entity("HRSI",fileName,"Parsing Failed",3,myblob.name,False,err_msg)


        try:
            azStorage.azure_upsert_entity(event_client, metadata)
        except:
            logging.info("There is no ROWKEY (alias FileName) in the EventMetadata Table for Update")
