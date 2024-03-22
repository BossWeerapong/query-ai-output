# import library
import os
import pandas as pd
import pymysql
from dotenv import load_dotenv

# load environment credential
load_dotenv()


def get_data_df(host, port, user, password, database_name, table_name) :
    ''' connect database and return to dataframe

    Paramerters :
        host : host for connect database
        port : port for connect database
        user : user for connect database
        password : password for connect database
        database : database name for connect database
        table_name : table_name for connect database

    Returns:
        data_df : dataframe for show data
    '''

    # 1. connect data base
    connection = pymysql.connect( host = host,
                                  port = int(port),
                                  user = user,
                                  passwd = password,
                                  db = database_name )

    # 2. get data ai output for each confident
    sql_query = f"SELECT * FROM {table_name}"
    data_df = pd.read_sql(sql_query, connection)

    connection.commit()

    return data_df.fillna('')

# connect ai_input_conversation_level table in ai_input database
query_df = get_data_df(host = os.environ.get("DAtABASE_HOST"),
                       port = os.environ.get("DAtABASE_PORT"), 
                       user = os.environ.get("DAtABASE_USER"), 
                       password = os.environ.get("DAtABASE_PASSWORD"),
                       database_name = "ai_input",
                       table_name = "ai_input_conversation_level")


# defind categories and model intents
categories_df = pd.read_excel('CaseType_PEA.xlsx').fillna('')
intents_df = pd.read_excel('Input_IntentModel.xlsx').fillna('')

caterogies = []
for index_row , row in categories_df.iterrows() :
    levels = [row['caselevel1'].strip(), row['caselevel2'].strip(), row['caselevel3'].strip(), row['caselevel4'].strip()]
    caterogy = ' > '.join([level for level in levels if level != ''])
    caterogies.append(caterogy)
model_intents = []
for index_row ,row in intents_df.iterrows() :
    intent = ' > '.join([row['model'], row['intent']])
    model_intents.append(intent)


# aws lambda function
def lambda_handler(event, context):
    try : 
        ref_code = '0000'
        start_date = query_df['create_datetime'].min().split(' ')[0]
        end_date = query_df['create_datetime'].max().split(' ')[0]
        category = caterogies
        number_of_false_items = 1
        ai_score = [str(i) for i in range(70,100,5)]
        limit_rows = 200

        parameters = [{"parameter" : "ref_code", "type" : "str", "values" : ref_code},
                      {"parameter" : "start_date", "type" : "str", "values" : start_date},
                      {"parameter" : "end_date", "type" :"str", "values" : end_date},
                      {"parameter" : "category", "type" : "list", "values" : category},
                      {"parameter" : "model_intents", "type" : "list" , "values" : model_intents},
                      {"parameter" : "ai_score", "type" : "str" , "values" : ai_score},
                      {"parameter" : "number_of_false_items", "type" : "int", "values" : number_of_false_items},
                      {"parameter" : "limit_rows", "type" : "int", "values" : limit_rows}]
        
        return {
            'statusCode': 200,
            'status' : 'success',
            'results': parameters
            }
    
    
    except Exception as e:
        return {
            'statusCode': 500,
            'status' : 'fail',
            'error_msg': 'An error occurred: {}'.format(e) 
            }