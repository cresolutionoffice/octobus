from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import re


es = Elasticsearch(
    hosts=['https://db.qa.naus1.ccloud.octobus.tools.sap:443/'],
    api_key=(<your own key>)
)


def get_data_from_elastic():
    # query: The elasticsearch query.
    # source will specify which fields you want to write to a file
    # you can see the query directly in elasticsearch by going to octobus, click on inspect, click on request, and copy the query, replace only the correspoonding query lines below
    query = {
    "_source" : ['BODY_ES.COMPLETION_DELAY_CALCULATED', 'BODY_ES.COMPLETION_DELAY_DAYS', 'BODY_ES.COMPLETION_DELAY_HOURS', 'BODY_ES.DEV_ORGANISATION_ID', 'BODY_ES.ES_BPO_ID', 'BODY_ES.ES_EC_ID', 'BODY_ES.ES_ID', 'BODY_ES.ES_SALES_ORG_SHORT', 'BODY_ES.FR_CHANGED_DATE', 'BODY_ES.FR_CONTRACT_START_DATE', 'BODY_ES.FR_CREATED_ON', 'BODY_ES.FR_DATA_CENTER', 'BODY_ES.FR_DATA_CENTER_DESCR', 'BODY_ES.FR_DESCR', 'BODY_ES.FR_ID', 'BODY_ES.FR_OP_TYPE', 'BODY_ES.FR_OP_TYPE_DESCR', 'BODY_ES.FR_PHASE_START_DATE', 'BODY_ES.FR_REASON_COMPLETION', 'BODY_ES.FR_REASON_COMPLETION_CATEGORY', 'BODY_ES.FR_TPT', 'BODY_ES.FR_TPT_DESCR', 'BODY_ES.FR_TPT_LEAD_TIME', 'BODY_ES.FR_TPT_LOB','BODY_ES.FR_TPT_LOB_DESCR', 'BODY_ES.FR_TPT_PROV_SCENARIO', 'BODY_ES.FR_TPT_PROV_SCENARIO_DESCR', 'BODY_ES.FR_ZHCODE', 'BODY_ES.FR_ZHCODE_DESCR', 'BODY_ES.IN_PROCESS_CALCULATED', 'BODY_ES.IN_PROCESS_DAYS', 'BODY_ES.IN_PROCESS_HOURS', 'BODY_ES.OPS_ORGANIZATION_ID', 'BODY_ES.PARENT_ITEM_TYPE', 'BODY_ES.SOLUTION_AREA', 'BODY_ES.FR_TENANT_ID', 'NAME', 'PROJECT'],
    "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "range": {
            "@timestamp": {
              "format": "strict_date_optional_time",
              "gte": "2024-07-24T04:00:00.000Z",
              "lte": "2024-08-23T16:58:25.367Z"
            }
          }
        },
        {
          "match_phrase": {
            "PROJECT": "cffcrm"
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
  }
    # Scan function to get all the data. using a chunking method below with specified index below, the index will change depending on which index you are querying
    rel = scan(client=es,             
               query=query,                                     
               scroll='1m',
               size=10000,
               index='*c0065_log_callidus_ies*',
               raise_on_error=True,
               preserve_order=False,
               clear_scroll=True)


    # Keep response in a list.
    result = list(rel)


    temp = []


    # We need only '_source', which has all the fields required.
    # This elimantes the elasticsearch metdata like _id, _type, _index.


    for hit in result:
        temp.append(hit['_source'])


    df = pd.DataFrame(temp)
    # Remove values with regex pattern "\d*"
    #df = df.replace(to_replace=r'"\d*":', value='', regex=True)


    print(df)
    return df



df = get_data_from_elastic()
# return a file which has each row as a json object with no comma after each row and no bracket at beginning and end of file
df.to_json('./test_output2.json', orient='records', lines=True)


df.to_csv('./test_csv2.csv', sep='\t')


print(df)
