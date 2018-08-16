
# coding: utf-8

# In[31]:


import pandas as pd
import urllib.parse as ulp


# In[32]:


fetchSize = 100 # How many records to retrieve at once from the API
offset = 0
queryPt1 = 'sys_updated_on>javascript:gs.dateGenerate('
queyrPt2 = ')^ORDERBYsys_updated_on'


# In[33]:


ulp.urlencode(mydict)


# In[42]:


qStr = {'sysparm_exclude_reference_link' : 'true',
        'sysparm_query' : 0
        'sysparm_limit' : fetchSize, 
        'sysparm_offset': offset}

