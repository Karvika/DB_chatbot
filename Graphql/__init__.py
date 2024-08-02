import os
import azure.functions as func 
from flask import Flask, jsonify, request
import re
from azure.cosmos import CosmosClient, exceptions
import logging
import pyodbc
import openai
app = Flask(__name__)


azure_openai_endpoint = "your_azure_openai_endpoint"
azure_openai_key = "your_azure_openai_key"

# Configure OpenAI to use the Azure endpoint
openai.api_base = azure_openai_endpoint
openai.api_key = azure_openai_key
openai.api_type = 'azure'
openai.api_version = '2024-02-15-preview'

prompt_template_TEM = """
Your prompt that describe schema of your database.Eg. 
You are an expert in converting English questions to SQL queries. The database_IDM you are querying is a Microsoft SQL Server database_IDM with a table named 'Database name'. This table has the following columns:
Ignore the spelling mistakes and try to match the user input with the field names given below and give the result for the nearest matching column name from below fields.
The database has field
-ID (primary key, also known as employee id)
-Admin
-Owner
-client_name
-client_id
-Approvers (it can contain json structured data)


"""

prompt_template_IDM = """
You are an expert in converting English questions to SQL queries. The database_IDM you are querying is a Microsoft SQL Server database_IDM with a table named 'Database name'. This table has the following columns:
Ignore the spelling mistakes and try to match the user input with the field names given below and give the result for the nearest matching column name from below fields.
-ID (primary key, also known as employee id)
-Fiscal_year
-revenue
-Gross_savings
-Account_type
-Feedback_for_account (it can contain json structured data)

Convert the user's question into an SQL query. Here are some examples of questions and their corresponding SQL queries:

"""



@app.route('/home')
def home():    
    return 'Hello, Flask on Azure Functions!'



# Function to load Azure OpenAI Model and provide responses
def get_openai_response(question, prompt_template):
    prompt = f"{prompt_template}\n\n{question}"
    response = openai.Completion.create(
        engine="pgllmchatbot",  # Make sure to use the correct engine name
        prompt=prompt,
        max_tokens=250,
        temperature=0.5,
    )
    return response.choices[0].text.strip()



# Function to retrieve query from the Cosmos DB
def read_cosmos_query(sql):
    try:
        endpoint = os.environ['EMEAcosmosdbEndpoint']
        key = os.environ['EMEAcosmosdbKey']
        database_name = os.environ['EMEAdatabaseName']
        container_name = os.environ['GlobalTEMcontainer']

        client = CosmosClient(endpoint, key)
        database = client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        items = list(container.query_items(
            query=sql,
            enable_cross_partition_query=True
        ))
        columns = items[0].keys() if items else []
        rows = [list(item.values()) for item in items]
        return columns, rows
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB Error: {e}")
        return [], []


@app.post('/TEM')
def api_query_TEM():
    data = request.json
    question = data.get('question')
    
    if question:
        # question = re.sub(r'[^a-zA-Z0-9\s]+$', '', question).strip() + '?'
        question = re.sub(r'[^\w\s\'-]+$', '', question).strip()
        question += '?' if not question.endswith('?') else ''
        response = get_openai_response(question, prompt_template_TEM)
        first_query = response.split('\n')[0].replace("=>,->,-,=", "")
        # first_query = response.split('\n')[0]
        sanitized_query = first_query.replace("sql", "").replace("`", "").strip()
        
        try:
            columns, rows = read_cosmos_query(sanitized_query)
            if rows:
                human_readable_results = ""
                for row in rows:
                    result = dict(zip(columns, row))
                    # formatted_result = ", ".join(f"{key}: {value}" for key, value in result.items())
                    formatted_result = ", ".join(f"{value}" for value in result.values())
                    human_readable_results += f"{formatted_result},"
                
                

                return jsonify({
                    "query": sanitized_query,
                    "results": human_readable_results.strip().strip(',')
                })
            else:
                return return_empty_response()
        except exceptions.CosmosHttpResponseError as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Invalid request. 'question' field is required."}), 400

# Function to retrieve query from the MS SQL database
def read_sql_query(sql, connection_string_IDM):
    try:
        conn = pyodbc.connect(connection_string_IDM)
        cursor = conn.cursor()
        logging.info(f"Executing SQL query: {sql}")
        sql = sql.strip('<|im_end|>')
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        conn.commit()
        conn.close()
        return columns, rows
    except pyodbc.Error as e:
        logging.error(f"SQL Error: {e}")
        return [], []



@app.post('/IDM')
def api_query_IDM():
    data = request.json
    question = data.get('question')
    
    if question:
        question = re.sub(r'[^\w\s\'-]+$', '', question).strip()
        question += '?' if not question.endswith('?') else ''
        response = get_openai_response(question, prompt_template_IDM)
        first_query = response.split('\n')[0].replace("=> ,->,-,=", "")
        # first_query = response.split('\n')[0]
        sanitized_query = first_query.replace("sql", "").replace("`", "").strip()
        
        try:# Connection paraTEMers
            server = os.environ['server']
            username = os.environ['SQLusername']
            password = os.environ['password']
            database_IDM = os.environ['database_IDM']
            connection_string_IDM = (
                "DRIVER={ODBC Driver 18 for SQL Server};"
                f"Server=tcp:{server},1433;"
                f"Database={database_IDM};"
                f"Uid={username};"
                f"Pwd={password};"
                "Encrypt=yes;"
                "TrustServerCertificate=no;"
                "Connection Timeout=60;"
            )
            columns, rows = read_sql_query(sanitized_query, connection_string_IDM)
            if rows:
                human_readable_results = ""
                for row in rows:
                    result = dict(zip(columns, row))
                    # formatted_result = ", ".join(f"{key}: {value}" for key, value in result.items())
                    formatted_result = ", ".join(f"{value}" for value in result.values())
                    human_readable_results += f" {formatted_result},"
            

                return jsonify({
                    "query": sanitized_query,
                    "results": human_readable_results.strip().strip(',')
                })
            else:
                return return_empty_response()
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        return jsonify({"error": "Invalid request. 'question' field is required."}), 400

def return_empty_response():
    response_data = {
        "query": "",
        "results": ""
    }
    return jsonify(response_data)

def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    """Each request is redirected to the WSGI handler.
    """
    logging.error("function app entered")
    return func.WsgiMiddleware(app.wsgi_app).handle(req, context)
