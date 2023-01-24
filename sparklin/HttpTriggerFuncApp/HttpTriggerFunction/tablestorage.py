import os
from azure.data.tables import TableServiceClient

class tablestorage:
    
    def __init__(self) -> None:
        # connstr = os.environ["LINEAGE_STORAGE_CONN_STR"]
        self.connstr = "DefaultEndpointsProtocol=https;AccountName=datalineagesynapsestrpoc;AccountKey=<<Provide your Storage Account Key>>;EndpointSuffix=core.windows.net"
        # tablename = os.environ["LINEAGE_EVENT_TABLE"]
        self.tablename = "EventMetadataYJ"

        self.table_service_client = TableServiceClient.from_connection_string(self.connstr)
        self.table_service_client.create_table_if_not_exists(self.tablename)
        

    def insertEventMetadata(self, eventrow) -> None:
        table_client = self.table_service_client.get_table_client(self.tablename)
        table_client.create_entity(eventrow)
