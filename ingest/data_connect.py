#!/usr/bin/env python
# coding: utf-8

# In[64]:


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus


# In[52]:


scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]
creds = ServiceAccountCredentials.from_json_keyfile_name('data-ingestion-464816-ae573e49660d.json', scope)


# In[53]:


client = gspread.authorize(creds)
sheet = client.open("rental_car_issues").sheet1  # or use .open_by_url()
data = sheet.get_all_records()


# In[54]:


import pandas as pd
df = pd.DataFrame(data)
print(df.head())


# In[55]:


df.info


# In[56]:


df.describe()


# In[57]:


load_dotenv(dotenv_path='cred.env')


# In[67]:


host=os.getenv("MYSQL_HOST")
user=os.getenv("MYSQL_USER")
password=os.getenv("MYSQL_PASSWORD")
password = quote_plus(password)
database=os.getenv("MYSQL_DB")
port = 3306

print(password)
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


# In[68]:


df.to_sql('issues_raw', con=engine, if_exists='replace', index=False)


# In[ ]:




