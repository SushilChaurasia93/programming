#!/usr/bin/env python
# coding: utf-8

# In[11]:


import pandas as pd
import os
import numpy as np
import datetime as dt
from datetime import datetime, date, timedelta


# In[12]:


#pip install --upgrade google-cloud-BigQuery
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes


# In[13]:


credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
client = bigquery.Client(credentials=credentials)


# # Target Load Functions

# In[24]:


def DimCustomerLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_dim_customer`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('dim_customer')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0


# In[21]:


def DimAddressLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_dim_address`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('dim_address')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0


# In[25]:


def DimOrderLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_dim_order`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('dim_order')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0


# In[26]:


def DimProductLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_dim_product`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('dim_product')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0


# In[27]:


def factOrderDetailsLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_f_order_details`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('f_order_details')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0


# In[28]:


def factDailyOrdersLoad():    
    query_string ="""SELECT *
                    FROM `united-bongo-351617.stg_UFH.stg_fact_daily_orders`
                    """
    df = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    tableRef = client.dataset("UFH").table('fact_daily_orders')
    bigqueryJob = client.load_table_from_dataframe(df, tableRef)
    return 0



DimAddressLoad()

DimCustomerLoad()

DimOrderLoad()

DimProductLoad()

factOrderDetailsLoad()

factDailyOrdersLoad()




