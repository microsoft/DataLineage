import azure.data.tables

class event():

    Status = 'Unprocessed'
    Message = ''
    RetryCount = 3
    FilepPath = '/openlineage/'
    isArchived = 0

    def __init__(self, teamname: str, filename: str) -> None:
        self.PartitionKey = teamname
        self.RowKey = filename
