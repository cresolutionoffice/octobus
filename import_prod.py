from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import re


es = Elasticsearch(
    hosts=['https://db.octobus.tools.sap:443/'],
    api_key=(<your own key>)
)

def get_data_from_elastic():
    # query: The elasticsearch query.
    # source will specify which fields you want to write to a file
    # you can see the query directly in elasticsearch by going to octobus, click on inspect, click on request, and copy the query, replace only the correspoonding query lines below
    query = {
    "_source" : ['_id', '_score'], 
    "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "range": {
            "@timestamp": {
              "format": "strict_date_optional_time",
              "gte": "2024-09-09T20:00:00.000Z",
              "lte": "2024-09-10T20:40:00.833Z"
            }
          }
        },
        {
          "match_phrase": {
            "project": "CPQ Error Logs"
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
               index='*:c0065*',
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
