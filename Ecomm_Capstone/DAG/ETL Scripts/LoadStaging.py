#!/usr/bin/env python
# coding: utf-8

# In[82]:


import psycopg2
from psycopg2 import extras 
import pandas as pd
import os
import numpy as np
import datetime as dt
from datetime import datetime, date, timedelta

# In[83]:


#pip install --upgrade google-cloud-BigQuery
from google.cloud import bigquery
from google.oauth2 import service_account
import db_dtypes


credentials = service_account.Credentials.from_service_account_file('/home/airflow/gcs/data/googlekey.json')
client = bigquery.Client(credentials=credentials)


#connection to source Postgres DB
conn = psycopg2.connect(database="ecomm",
                        user='sushil', password='admin123', 
                        host='34.73.147.246', port='5432'
)

conn.autocommit = True
cursor = conn.cursor()




#default date
format_data = "%d/%m/%Y %H:%M:%S.%f"
time_data = "01/01/1900 00:00:0.000"
default_date = datetime.strptime(time_data,format_data)


# # Function to insert ETL runtime logs

# In[88]:


def insertetllog(logid, tblname, rowcount, status):
    try:
        # set record attributes
        record = {"processlogid":logid,"tablename":tblname,"extractrowcount": rowcount,
                  "lastextractdatetime":datetime.now(),"status":status}
        print(record)
        logs = pd.DataFrame(record, index=[0])
        log_tbl_name = "etlextractlog"
        
        df_columns = list(logs)
        columns = ",".join(df_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns])) 
        insert_stmt = "INSERT INTO {} ({}) {}".format(log_tbl_name,columns,values)
        psycopg2.extras.execute_batch(cursor, insert_stmt, logs.values)
        conn.commit()
    except Exception as e:
        print("Unable to insert record into etl logs" + print(str(e)))


# In[89]:


def getLastETLRunDate(tblName):
    try:
        qry = f"""Select  max(lastextractdatetime) as lastETLRunDate
        from etlextractlog where tablename= '{tblName}' and status='Completed'"""
        cursor.execute(qry)
        for i in cursor.fetchall():
            etlrundate = i[0]
            if not etlrundate:
                etlrundate = default_date
            return etlrundate
    except Exception  as e:
        return default_date


# In[107]:


def upsert(df, tbl, key):
    try:
        print('Staging Load Started')
        tableRef = client.dataset("stg_UFH").table(tbl)
        bigqueryJob = client.load_table_from_dataframe(df, tableRef)
        #bigqueryJob.result()
        print('Load Completed')
        insertetllog(1, tbl, len(df), "Completed")
    except Exception as e:  
        insertetllog(1, tbl, len(df), "Failed")
    


# # Extract Functions

# In[91]:


def CustomerExtract(lastrundate):    
    df = pd.read_sql(f'''select * from customer_master where update_timestamp >= '{lastrundate}';''',conn)
    df.index=np.arange(1,len(df)+1)
    return df


# In[92]:


def OrdersExtract(lastrundate):    
    df = pd.read_sql(f'''select * from order_details where order_status_update_timestamp >= '{lastrundate}';''',conn)
    df.index=np.arange(1,len(df)+1)
    return df


# In[93]:


def ProductExtract(lastrundate):
    df = pd.read_sql(f'''select * from product_master;''',conn)
    df.index=np.arange(1,len(df)+1)    
    return df


# In[153]:


def OrdersItemsExtract(order_lastrundate):
    df = pd.read_sql(f'''select * from order_items where orderid in 
                        (select distinct orderid from order_details where order_status_update_timestamp>= '{order_lastrundate}'
                         and order_status ='InProgress');''',conn)
    df.index=np.arange(1,len(df)+1)    
    return df


# # Transform functions

# In[95]:


def AddressTransform(delta_customer): 
    dim_address_fields = ['address_id','address','city','state','pincode']
    dim_address = pd.DataFrame(columns = dim_address_fields)
    delta_address = pd.DataFrame(columns = dim_address_fields,index=range(1,len(delta_customer)+1))
    
    
    #fetch hisotrical records
    query_string ="""SELECT *
                    FROM `united-bongo-351617.UFH.dim_address`
                    ORDER BY address_id"""
    dim_address = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    
    query_string ="""SELECT coalesce(max(address_id),0) as max_address_id
                    FROM `united-bongo-351617.UFH.dim_address`
                    """
    max_address = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    
    
    #fetch delta records    
    for i in range(1,len(delta_customer)+1):
        delta_address['address_id'][i] = max_address['max_address_id'][0]+1
        delta_address['address'][i] = delta_customer['address'][i]
        delta_address['city'][i] = delta_customer['city'][i]
        delta_address['state'][i] = delta_customer['state'][i]
        delta_address['pincode'][i] = delta_customer['pincode'][i]
        
        max_address['max_address_id'][0] = max_address['max_address_id'][0]+1
        
    #compare 
    inserts = delta_address[~delta_address.apply(tuple,1).isin(dim_address.apply(tuple,1))]
    return inserts


# In[96]:


def CustomerTransform(delta_customer):
    dim_customer_fields = ['customerid','name','address_id','start_date','end_date']
    dim_customer = pd.DataFrame(columns = dim_customer_fields)
    
    #fetch hisotrical records
    query_string ="""SELECT *
                        FROM `united-bongo-351617.UFH.dim_customer` c
                        ORDER BY customerid"""
    dim_customer = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    
    
    #lookup for address_id field    
    query_string2 ="""SELECT * FROM `united-bongo-351617.UFH.dim_address` 
                    UNION ALL
                    select * from `united-bongo-351617.stg_UFH.stg_dim_address`"""
    address = (client.query(query_string2).result().to_dataframe(create_bqstorage_client=False,))
           
    df1=pd.merge(delta_customer,address,on=['address','city','state','pincode'],how='left')
    df1.index=np.arange(1,len(df1)+1)
    
    df1['start_date'] = pd.to_datetime(delta_customer["update_timestamp"],errors = 'ignore').dt.date
    df1['end_date'] = pd.to_datetime(date(9999, 12, 31),errors = 'ignore')
    delta_customer1 = df1[['customerid','name','address_id','start_date','end_date']]
    
   
    updates = delta_customer1[delta_customer1.customerid.isin(dim_customer.customerid)]
    inserts = delta_customer1[~delta_customer1.customerid.isin(dim_customer.customerid)]
    
    inserts['start_date'] = inserts['start_date'].astype('datetime64[ns]',errors = 'ignore')
    inserts['end_date'] = inserts['end_date'].astype('datetime64[ns]',errors = 'ignore')
    return inserts,updates
    


# In[97]:


def OrdersTransform(delta_orders): 
    dim_order_fields = ['orderid','order_status_update_timestamp','order_status']
    dim_order = pd.DataFrame(columns = dim_order_fields)
    dim_order = delta_orders.drop('customerid',axis=1)
    return dim_order


# In[98]:


def FactOrderDetailsTransform(delta_orders,delta_order_items):
    delivered_df = delta_orders.where(delta_orders['order_status']=='Delivered').dropna()[['customerid','orderid','order_status_update_timestamp']]
    df = pd.merge(delta_order_items,delivered_df,on='orderid',how='left')
    df = df.drop(['customerid'],axis=1)
    df.rename(columns = {'order_status_update_timestamp':'order_delivery_timestamp'}, inplace = True)
    df.index=np.arange(1,len(df)+1) 
    
    return df
    
    


# In[165]:


def FactDailyOrdersTransform(delta_orders,delta_customer,delta_products,fact_order_details):
    fields = ['customerid','orderid','order_received_timestamp','order_delivery_timestamp','pincode','order_amount','item_count','order_delivery_time_seconds']
    fact_daily_orders = pd.DataFrame(columns=fields)
    
    received_df = delta_orders.where(delta_orders['order_status']=='Received').dropna()[['customerid','orderid','order_status_update_timestamp']]
    delivered_df = delta_orders.where(delta_orders['order_status']=='Delivered').dropna()[['customerid','orderid','order_status_update_timestamp']]
    temp_df = pd.merge(received_df, delivered_df, on=['customerid','orderid'], how='left')
    
    temp_df['order_delivery_time_seconds']=pd.to_datetime(temp_df['order_status_update_timestamp_y'])-pd.to_datetime(temp_df['order_status_update_timestamp_x'])
    temp_df['order_delivery_time_seconds']=temp_df['order_delivery_time_seconds']/np.timedelta64(1,'s')
    
    temp_df.rename(columns = {'order_status_update_timestamp_x':'order_received_timestamp',
                        'order_status_update_timestamp_y':'order_delivery_timestamp'}, inplace = True)
    
    temp_df = pd.merge(temp_df, delta_customer, on=['customerid'], how='left')
    temp_df = temp_df.drop(['name','address','city','state','update_timestamp'],axis=1)
    
    #df= pd.merge(temp_df,delta_products, on ='productid',how='left')
    
    #Derive Order_amount column
    df= pd.merge(fact_order_details,delta_products, on ='productid',how='left')
    df['order_amount'] = df['quantity']*df['rate']
    df = df.drop(['productcode','productname','sku','isactive'],axis=1)
    
    amounts= df[['orderid','order_amount']]
    amounts= amounts.groupby('orderid').sum()
    
    quants = df[['orderid','quantity']]
    quants= quants.groupby('orderid').sum()
    
    temp_df = pd.merge(temp_df,amounts,on='orderid',how='left')
    fact_daily_orders = pd.merge(temp_df,quants,on='orderid',how='left')
    fact_daily_orders.rename(columns = {'quantity':'item_count'}, inplace = True)
    fact_daily_orders.fillna(0, inplace=True)
    
    return fact_daily_orders
    
    


# In[101]:


def ProductTransform(delta_products):
    
    query_string ="""SELECT productid,productcode,productname,sku,rate,isactive
                    FROM `united-bongo-351617.UFH.dim_product`
                    ORDER BY productid"""
    
    dim_product = (client.query(query_string).result().to_dataframe(create_bqstorage_client=False,))
    
    changes = delta_products[~delta_products.apply(tuple,1).isin(dim_product.apply(tuple,1))]
    updates = changes[changes.productid.isin(dim_product.productid)]
    inserts = changes[~changes.productid.isin(dim_product.productid)]
    
    inserts['start_date'] = datetime.now().date()
    inserts['end_date'] = pd.to_datetime(date(9999, 12, 31),errors = 'ignore')
    
    inserts['start_date'] = inserts['start_date'].astype('datetime64[ns]',errors = 'ignore')
    inserts['end_date'] = inserts['end_date'].astype('datetime64[ns]',errors = 'ignore')
    
    updates['end_date'] = datetime.now().date()
    
    return inserts, updates
    
    


# In[ ]:





# # Execute Extract & Transform Functions

# In[154]:


#Call customer Extarct function
lastrundate = getLastETLRunDate('stg_dim_customer')
delta_customer = CustomerExtract(lastrundate)

#Call order Extarct function
order_lastrundate = getLastETLRunDate('stg_dim_order')
delta_orders = OrdersExtract(order_lastrundate)

#Call product Extract function
lastrundate = getLastETLRunDate('stg_dim_product')
delta_products = ProductExtract(lastrundate)

#call Order Items Extract function
delta_order_items = OrdersItemsExtract(order_lastrundate)


# In[109]:


#Transforms


# In[134]:


#transform and load address
address_inserts=AddressTransform(delta_customer)
upsert(address_inserts,'stg_dim_address','address_id')


# In[135]:





# In[136]:


#transform and load orders
orders_insert=OrdersTransform(delta_orders)
upsert(orders_insert,'stg_dim_order','orderid')


# In[137]:


#transform and load products
product_insert, product_updates=ProductTransform(delta_products)
upsert(product_insert,'stg_dim_product','productid')


# In[138]:


#transform and load fact_order_details
fact_order_details = FactOrderDetailsTransform(delta_orders, delta_order_items)
upsert(fact_order_details,'stg_f_order_details','orderid')


# In[167]:


#transform and load fact_daily_orders
fact_daily_orders = FactDailyOrdersTransform(delta_orders,delta_customer,delta_products,fact_order_details)
upsert(fact_daily_orders,'stg_fact_daily_orders','orderid')


# In[ ]:

#transform and load customer
cust_inserts,cust_updates = CustomerTransform(delta_customer)
upsert(cust_inserts,'stg_dim_customer','customerid')


