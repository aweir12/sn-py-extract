
# coding: utf-8

# In[1]:


import pandas as pd
import urllib.parse as ulp
import params


# In[2]:


# Local Variables
fetchSize = 100 # How many records to retrieve at once from the API
offset = 0
queryPt1 = 'sys_updated_on>javascript:gs.dateGenerate('
queyrPt2 = ')^ORDERBYsys_updated_on'


# In[3]:


# Global Variables
baseURL = params.baseURL


# In[4]:


ulp.urlencode(mydict)


# In[5]:


qStr = {'sysparm_exclude_reference_link' : 'true',
        'sysparm_query' : 0,
        'sysparm_limit' : fetchSize, 
        'sysparm_offset': offset}

