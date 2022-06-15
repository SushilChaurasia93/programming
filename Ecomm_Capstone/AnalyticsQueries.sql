1. Average number of items per order - daily, monthly, weekly, state, city, pincode

Select 
#EXTRACT(DAY from order_received_timestamp) AS Daily,
#EXTRACT(MONTH from order_received_timestamp) AS Monthly,
#EXTRACT(WEEK from order_received_timestamp) AS Weekly,
#a.state AS State,
#a.city AS City,
f.pincode AS Pincode,
avg(item_count) as avg_items

From  `united-bongo-351617.UFH.fact_daily_orders` f
left join `united-bongo-351617.UFH.dim_customer` c
  on f.customerid = c.customerid
left join `united-bongo-351617.UFH.dim_address` a
  on c.address_id = a.address_id
group by 
#Daily
#Monthly
#Weekly
#STATE
#City
Pincode



2. Average amount of sales per order -  daily, monthly, weekly, state, city, pincode

Select 
#EXTRACT(DAY from order_received_timestamp) AS Daily,
#EXTRACT(MONTH from order_received_timestamp) AS Monthly,
#EXTRACT(WEEK from order_received_timestamp) AS Weekly,
#a.state AS State,
#a.city AS City,
f.pincode AS Pincode,
avg(order_amount) as avg_sales

From  `united-bongo-351617.UFH.fact_daily_orders` f
left join `united-bongo-351617.UFH.dim_customer` c
  on f.customerid = c.customerid
left join `united-bongo-351617.UFH.dim_address` a
  on c.address_id = a.address_id
group by 
#Daily
#Monthly
#Weekly
#STATE
#City
Pincode


3. total number of units sold per day of a product SKU and its monthly trend

select
p.productname,p.sku,
EXTRACT(DAY from order_delivery_timestamp) DAY,
#EXTRACT(MONTH from order_delivery_timestamp) MONTH,
sum(quantity) as units_sold
From  `united-bongo-351617.UFH.dim_product` p 
join `united-bongo-351617.UFH.f_order_details` f
on p.productid = f.productid
group by productname,
sku,
DAY
#,MONTH


4. Total Order Amount on daily basis, also to be able to split by product and geography

select 
EXTRACT(DATE from order_received_timestamp) AS Daily,
o.productid,
a.city,
sum(order_amount) total_sales
from `united-bongo-351617.UFH.fact_daily_orders` f
join `united-bongo-351617.UFH.f_order_details` o on f.orderid = o.orderid 
join `united-bongo-351617.UFH.dim_customer` c on f.customerid = c.customerid
join `united-bongo-351617.UFH.dim_address` a on c.address_id = a.address_id
group by 
Daily,
productid,
city
order by Daily


5. Distribution of orders according to area ( state, city, pincode etc)

select 
a.state,
a.city,
count(distinct orderid) NumberOfOrders
from `united-bongo-351617.UFH.fact_daily_orders` f
join `united-bongo-351617.UFH.dim_customer` c on f.customerid = c.customerid
join `united-bongo-351617.UFH.dim_address` a on c.address_id = a.address_id
group by 
state,city


6. Average order amount per customer on daily basis

select 
customerid,
EXTRACT(DATE from order_received_timestamp) Date,
round(avg(order_amount),2) AS OrderAmount
from `united-bongo-351617.UFH.fact_daily_orders` f
group by 
customerid,Date


7. New Customers on daily basis

select 
START_DATE,
count(customerid) NewCustomers
from `united-bongo-351617.UFH.dim_customer`
where customerid in (select customerid from `united-bongo-351617.UFH.dim_customer`
group by customerid having count(*)=1)
group by START_DATE


8. Total count of customers everyday

select distinct
EXTRACT(DATE from order_received_timestamp) Dates,
count(*) over (partition by EXTRACT(DATE from order_received_timestamp)) CustomerCounts
from `united-bongo-351617.UFH.fact_daily_orders` f
group by 
order_received_timestamp
order by Dates


9. Average time to delivery order. Min and Max time. To be able to slice and dice on hour, weekday, weekend, daily, monthly, geography, 

select distinct
EXTRACT(DATE from order_delivery_timestamp) DATES,
EXTRACT(WEEK from order_delivery_timestamp) WEEKS,
EXTRACT(DAYOFWEEK from order_delivery_timestamp) WEEKDAYS,
EXTRACT(MONTH from order_delivery_timestamp) MONTHS,
a.City,
Min(order_delivery_time_seconds) MinDeliveryTime,
max(order_delivery_time_seconds) MaxDeliveryTime,
avg(order_delivery_time_seconds) AvgDeliveryTime,
from `united-bongo-351617.UFH.fact_daily_orders` f
join `united-bongo-351617.UFH.dim_customer` c on f.customerid = c.customerid
join `united-bongo-351617.UFH.dim_address` a on c.address_id = a.address_id
group by 
DATES,
WEEKS,
WEEKDAYS,
MONTHS,
City


10. Total orders : to be able to slice and dice on hour, weekday, weekend, daily, monthly, geography

select distinct
EXTRACT(DATE from order_delivery_timestamp) DATES,
EXTRACT(WEEK from order_delivery_timestamp) WEEKS,
EXTRACT(DAYOFWEEK from order_delivery_timestamp) WEEKDAYS,
EXTRACT(MONTH from order_delivery_timestamp) MONTHS,
a.City,
count(orderid) NumberOfOrders
from `united-bongo-351617.UFH.fact_daily_orders` f
join `united-bongo-351617.UFH.dim_customer` c on f.customerid = c.customerid
join `united-bongo-351617.UFH.dim_address` a on c.address_id = a.address_id
group by 
DATES,
WEEKS,
WEEKDAYS,
MONTHS,
City
