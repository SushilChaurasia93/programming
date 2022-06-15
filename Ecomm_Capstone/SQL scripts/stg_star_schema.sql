create or replace table `united-bongo-351617.stg_UFH.stg_dim_order`
(orderid integer,
order_status_update_timestamp timestamp,
order_status string);


create or replace table `united-bongo-351617.stg_UFH.stg_dim_customer`
(customerid integer,
name string,
address_id integer,
start_date date,
end_date date);


create or replace table `united-bongo-351617.stg_UFH.stg_dim_address`
(address_id integer,
address string,
city string,
state string,
pincode integer);


create or replace table `united-bongo-351617.stg_UFH.stg_dim_product`
(productid integer,
productcode string,
productname string,
sku string,
rate integer,
isactive boolean,
start_date date,
end_date date);


create or replace table `united-bongo-351617.stg_UFH.stg_fact_daily_orders`
(customerid	integer,
orderid	integer,
order_received_timestamp timestamp,
order_delivery_timestamp timestamp,
pincode	integer,
order_amount integer,
item_count integer,
order_delivery_time_seconds integer);



create or replace table `united-bongo-351617.stg_UFH.stg_f_order_details`
(orderid integer,
order_delivery_timestamp timestamp,
productid integer,
quantity integer)
