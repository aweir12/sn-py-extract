
# coding: utf-8

# In[1]:


import pandas as pd
import os
import urllib.parse as ulp
import re
import params
from sqlalchemy import create_engine
import sqlalchemy.types
import requests


# In[2]:


# Local Variables
fetchSize = 500 # How many records to retrieve at once from the API
queryPt1 = 'sys_updated_on>javascript:gs.dateGenerate('
lastUpdateDate = "'2012-01-01','12:00:00'"
queryPt2 = ')^ORDERBYsys_updated_on'
os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8" # Define characterset, thanks Oracle
qStr = {'sysparm_exclude_reference_link' : 'true',
        'sysparm_query' : 0,
        'sysparm_limit' : fetchSize, 
        'sysparm_offset': 0}


# In[3]:


# Global Variables
baseURL = params.baseURL
username = params.username
password = params.password
snHeaders = params.snHeaders
connection = params.dbURL + ulp.urlencode(params.dbParams)
engine = create_engine(connection) # Create Database Engine


# In[4]:


# Retrieve Date/Time of the Newest Record in the Database for a Table
def getLastRefreshDate(dbTableIn):
    query = "SELECT TO_CHAR(MAX(SYS_UPDATED_ON - (4)), '''YYYY-MM-DD'',''HH24:MI:SS''') FROM " + dbTableIn
    try:
        lastUpdateDate = "'" + engine.execute(query).first()[0].replace(' ',"','") + "'"
    except:
        lastUpdateDate = "'2012-01-01','12:00:00'"
    return lastUpdateDate


# In[5]:


# Build the URL Call for the ServiceNow Table API
def buildURL(snTableIn, lastUpdateDateIn, offsetIn=0):
    # Setup Incremental Refresh Here
    query = queryPt1 + lastUpdateDateIn + queryPt2
    qStr['sysparm_query'] = query
    qStr['sysparm_offset'] = offsetIn
    urlQuery = ulp.urlencode(qStr)
    urlCall = baseURL + snTableIn + '?' + urlQuery
    return urlCall


# In[6]:


# Determine Column Types for Oracle DB
# This could use some work, and some documentation
def dfReShape(dfIn):
    colNames = list(dfIn.columns)
    colTypes = {}
    for col in colNames:
        if(len(col)) > 30:
            colTypes[col] = re.sub(r'[AEIO]', '', col,flags = re.IGNORECASE)
    dfIn.rename(index = str, columns = colTypes, inplace = True)
    cols = dfIn.columns
    for cname in cols:
        if dfIn[cname].loc[dfIn[cname] != ''].count() > 0:
            try:
                dfIn[cname] = dfIn[cname].astype(int)
            except:
                try:
                    dfIn[cname] = pd.to_datetime(dfIn[cname]).dt.tz_localize('UTC').dt.tz_convert('US/Central').dt.tz_localize(None)
                except:
                    dfIn[cname] = dfIn[cname].astype(object)
    colTypes = {'sys_id' : sqlalchemy.types.VARCHAR(32)}
    strings = list(dfIn.select_dtypes(include=['object']).columns)
    for cname in strings:
        clength = dfIn[cname].map(lambda x: len(x)).max() + 10
        if clength < 4000:
            colTypes[cname] = sqlalchemy.types.VARCHAR(clength)
    out = (dfIn, colTypes)
    return out


# In[7]:


# Unload the ServiceNow Data & Shape the DF
def unloadData(snTableIn, dbTableIn):
    check = 0
    lastUpdateDate = getLastRefreshDate(dbTableIn)
    url = buildURL(snTableIn, lastUpdateDate)
    r = requests.get(url, auth=(username, password), headers=snHeaders)
    offset = len(r.json()['result'])
    df = pd.DataFrame(r.json()['result'])
    while offset != check:
        check = offset
        url = buildURL(snTableIn, lastUpdateDate, offset)
        r = requests.get(url, auth=(username, password), headers=snHeaders)
        if len(r.json()['result']) > 0:
            df = df.append(pd.DataFrame(r.json()['result']))
        offset = offset + len(r.json()['result'])
    if len(df) > 0:
        df.set_index('sys_id', inplace=True)
        dfOut = dfReShape(df)
    if len(df) < 1:
        dfOut = (df, {})
    return dfOut


# In[12]:


dev = unloadData('sys_user', 'arw_sys_user')
dev[0].to_sql('arw_sys_user',engine,chunksize=1500,if_exists='append', dtype=dev[1])

