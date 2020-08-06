import boto3
import os
from decimal import Decimal
import json
from botocore.exceptions import ClientError
import uuid
from boto3.dynamodb.conditions import Key


print('Loading function')
table_name = os.environ['TABLE_NAME']
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err["message"] if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }
    
def get_all_todos_for_user(user_id):
    response = table.query(
        KeyConditionExpression=
            Key('user_id').eq(user_id)
    )

    return response["Items"]  

def create_todos(todos,user,dynamodb=None):
    for todo in todos:
        item = {
            "user_id" : user,
            "todo_id" : str(uuid.uuid4()),
            "todo" : 
            {
                 "completed" : todo["completed"],
                 "content" : todo["content"]
             }
        }
        print("Adding todo",item)
        table.put_item(Item=item)
        print("Added")
        
def get_todo_by_id(user_id,todo_id,dynamodb):
    response = table.query(
        KeyConditionExpression=
            Key('user_id').eq(user_id) & Key('todo_id').eq(todo_id)
    )

    return response["Items"] 

def delete_todo_by_id(user_id,todo_id,dynamodb):
    try:
        response = table.delete_item(
            Key={
                'user_id' : user_id,
                'todo_id' : todo_id
            },
            ReturnValues = "ALL_OLD"
        )
    except ClientError as e:
        print('Exception occured while deleteing the todo',e)
    else:
        return response
        
def lambda_handler(event, context):
     print("loading function")
     operation = event['httpMethod']
     print("operation is",operation)
     print("event is ",event)
     print("context is ",context)
     resource = event['resource']
     if operation == "GET" and resource == "/todo":
         response = get_all_todos_for_user("anirvansen")
        
         if response:
             return respond(None,response)
         else:
             return respond({"message" : "No to do found for the user"})
             
     elif operation == "POST" and resource == "/todo":
         body = json.loads(event['body'])
         create_todos(body,"anirvansen",dynamodb)
         return respond(None,{"message" : "Successfully added!"})
         
     elif operation == "GET" and resource == "/todo/{todo_id}":
        todo_id = event['pathParameters'].get("todo_id")
        response = get_todo_by_id("anirvansen",todo_id,dynamodb)
        print('response is',response)
        if response:
             return respond(None,response)
        else:
            return respond({"message" : f"No to do found with todo_id = {todo_id}"})
     elif operation == "DELETE" and resource == "/todo/{todo_id}":
         todo_id = event['pathParameters'].get("todo_id")
         response = delete_todo_by_id("anirvansen",todo_id,dynamodb)
         if response.get("Attributes"):
             return respond(None,{"message" : "Successfully deleted!"})
         return respond({"message" : f"No to do found with todo_id = {todo_id}"})
         
     elif operation == "PUT" and resource == "/todo/{todo_id}":
         print('inside put operation')
         todo_id = event['pathParameters'].get("todo_id")
         user_id = "anirvansen"
         body = json.loads(event['body'])
         print('body is',body)
         todo_present = get_todo_by_id(user_id,todo_id,dynamodb)
         if todo_present:
             print('todo_present')
             response = table.update_item(
                           Key={
                               'user_id' : user_id,
                               'todo_id' : todo_id
                           },
                        UpdateExpression="set todo.content=:cont, todo.completed=:flag",
                        ExpressionAttributeValues={
                                ':cont': body['content'],
                                ':flag': body['completed']
                                },
                             ReturnValues="UPDATED_NEW"
                            )
             if response.get("Attributes"):
                return respond(None,response.get("Attributes"))
             return respond({"message" : "Something went wrong! Please try again"})
         else:
             return respond({"message" : f"Todo with todo_id = {todo_id} not present"})
        #  response = update_todo_by_id("anirvansen",todo_id,dynamodb)
        #  if response.get("Attributes"):
        #      return respond(None,{"message" : "Successfully deleted!"})
        #  return respond({"message" : f"No to do found with todo_id = {todo_id}"})
         
     return respond({"message" : f"Unsupported method {operation}"})