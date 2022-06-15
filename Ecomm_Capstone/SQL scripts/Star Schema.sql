create or replace table `united-bongo-351617.UFH.dim_order`
(orderid integer,
order_status_update_timestamp timestamp,
order_status string)
partition by DATE_TRUNC(order_status_update_timestamp,MONTH);


create or replace table `united-bongo-351617.UFH.fact_daily_orders`
(customerid	integer,
orderid	integer,
order_received_timestamp timestamp,
order_delivery_timestamp timestamp,
pincode	integer,
order_amount integer,
item_count integer,
order_delivery_time_seconds integer)
partition by DATE_TRUNC(order_received_timestamp,MONTH);


create or replace table `united-bongo-351617.UFH.dim_customer`
(customerid integer,
name string,
address_id integer,
start_date date,
end_date date)
partition by DATE_TRUNC(start_date,MONTH);


create or replace table `united-bongo-351617.UFH.dim_address`
(address_id integer,
address string,
city string,
state string,
pincode integer)
partition by RANGE_BUCKET(address_id, GENERATE_ARRAY(0, 1000, 100));


create or replace table `united-bongo-351617.UFH.dim_product`
(productid integer,
productcode string,
productname string,
sku string,
rate integer,
isactive boolean,
start_date date,
end_date date);


create or replace table `united-bongo-351617.UFH.f_order_details`
(orderid integer,
order_delivery_timestamp timestamp,
productid integer,
quantity integer)
partition by DATE_TRUNC(order_delivery_timestamp,MONTH);

