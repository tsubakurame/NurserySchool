import json
import boto3
from boto3.dynamodb.conditions import Key
import collections as cl

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("NurserySchoolItems")

class TTscItems:
    def __init__(self, json):
        self.__KeyList  = [ 'Date',
                            'Diapers', 
                            'PlasticBag', 
                            'Clothes',
                            'Towel',
                            'Bib',
                            'Underwear',
                            'Gauze',
                            'Pants',
                            'Socks',
                            'Hat',
                            'Remarks']
        self.__json = json
    
    @property
    def OperationType(self):
        return  self.__json['OperationType']
    @property
    def PartitionKey(self):
        return  self.__json['Keys']['Date']
    @property
    def JsonDump(self):
        list    = []
        dic = cl.OrderedDict()
        for i in self.__KeyList:
            try:
                dic[i] = self.__json['Keys'][i]
            except:
                dic[i] = '0'
        return dic
    
def operation_scan():
    scanData = table.scan()	
    items=scanData['Items']	
    print(items)	
    return scanData

def operation_query(partitionKey):
    queryData = table.query(
        KeyConditionExpression = Key("Date").eq(partitionKey)
    )
    # items=queryData['Items']
    # print(items)
    return queryData

def operation_put(Items):
    # putResponse = table.put_item(
    #     Item={
    #         'Date'      : Items.PartitionKey,
    #         'Diapers'   : Diapers,
    #         'PlasticBag': PlasticBag,
    #         'Clothes'   : Clothes,
    #         'Towel'     : Towel,
    #         'Bib'       : Bib,
    #         'Underwear' : Underwear,
    #         'Gauze'     : Gauze
    #     }
    putResponse = table.put_item(Item=Items.JsonDump)
    # )
    if putResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(putResponse)
    else:
        print('PUT Successed.')
    return putResponse

def operation_delete(partitionKey):
    delResponse = table.delete_item(
       key={
           'Date': partitionKey
       }
    )
    if delResponse['ResponseMetadata']['HTTPStatusCode'] != 200:
        print(delResponse)
    else:
        print('DEL Successed.')
    return delResponse

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event))
    # OperationType = event['OperationType']
    Items   = TTscItems(event)
    try:
        if Items.OperationType == 'SCAN':
            return operation_scan()
        # PartitionKey = event['Keys']['Date']
        if Items.OperationType == 'QUERY':
            return operation_query(Items.PartitionKey)
        elif Items.OperationType == 'PUT':
            # Diapers     = event['Keys']['Diapers']
            # PlasticBag  = event['Keys']['PlasticBag']
            # Clothes     = event['Keys']['Clothes']
            # Towel       = event['Keys']['Towel']
            # Bib         = event['Keys']['Bib']
            # Underwear   = event['Keys']['Underwear']
            # Gauze       = event['Keys']['Gauze']
            # Pants       = event['Keys']['Pants']
            # Socks       = event['Keys']['Socks']
            # Hat         = event['Keys']['Socks']
            # return operation_put(PartitionKey, Diapers, PlasticBag, Clothes, Towel, Bib, Underwear, Gauze)
            return operation_put(Items)
        elif Items.OperationType == 'DELETE':
            return operation_delete(Items.PartitionKey)
    except Exception as e:
        print("Error Exception.")
        print(e)