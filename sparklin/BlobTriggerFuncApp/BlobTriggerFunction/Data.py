import logging
from azure.data.tables import TableServiceClient, UpdateMode
# import os

class AZTableStorage:

    table_name = ""
    conn_str = ""
    table_client = ""

    def createClient(self, table_name=None, conn_str=None):
        self.table_name = table_name
        self.conn_str = conn_str
        print(">>>>>>>>table_name>>>>>>>>"+self.table_name)
        print(">>>>>>>>conn_str>>>>>>>>"+self.conn_str)
        self.table_service = TableServiceClient.from_connection_string(self.conn_str)

        # Create the table if it does not already exist
        self.table_service.create_table_if_not_exists(self.table_name)

        self.table_client = self.table_service.get_table_client(self.table_name)
        # logging.info(self.table_name)
        return self.table_client

    def insert_entity(self, tbl_client, entity):
        #entity = self.deserialize()
        return tbl_client.create_entity(entity)
    
    def azure_upsert_entity(self, tbl_client, entity):
        # print(entity)
        #entity = self.deserialize()        
        return tbl_client.upsert_entity(mode=UpdateMode.REPLACE, entity=entity)
    
    def azure_query_entities(self,tbl_client, queryfilter):
        return tbl_client.query_entities(queryfilter)

    # @staticmethod
    # def deserialize():
    #     params = {key: request.form.get(key) for key in request.form.keys()}
    #     params["PartitionKey"] = "Chicago"
    #     params["RowKey"] = "2021-07-01 12:00 AM"
    #     return params

    def create_event_entity(self, PartitionKey, RowKey, Status, RetryCount, FilePath, isArchived, Message):
        my_entity = {"PartitionKey" : str(PartitionKey),
                "RowKey" : str(RowKey),
                "Status" : str(Status),
                "RetryCount" : RetryCount,
                "FilePath" : str(FilePath),
                "isArchived" : isArchived,
                "Message" : str(Message)
                }
        return my_entity    

    def create_lineage_entity(self, PartitionKey, RowKey, input_tables, output_table, input_columns, output_columns, isdelta, isintermediate, isglobal, derived_columns, joinconditions):
        my_entity = {"PartitionKey" : str(PartitionKey),
                "RowKey" : str(RowKey),
                "input_tables" : str(input_tables),
                "output_table" : str(output_table),
                "input_columns" : str(input_columns),
                "output_columns" : str(output_columns),
                "isdelta" : str(isdelta),
                "isintermediate" : str(isintermediate),
                "isglobal" : str(isglobal),
                "derived_columns" : str(derived_columns),
                "joinconditions" : str(joinconditions)
                }
        return my_entity   
# def main():
#     name = "e4b54cdb-e6ca-432d-8edf-e584fec37611_sql_test_data_lineage_pool_1650000026883_20220527140329.json"
#     fileName = name[0:name.index(".")]
#     print(">>>>>>>>1>>>>>>>>"+fileName)
#     azStorage =  AZTableStorage()
#     azStorage.createClient()
#     metadata = azStorage.create_entity("Learning",fileName,"UnProcessed","Success",3,name,True)
#     azStorage.upsert_entity(metadata)
    
# if __name__ == "__main__":
#     main()