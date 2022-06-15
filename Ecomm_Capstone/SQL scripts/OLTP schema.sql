DROP TABLE IF EXISTS customer_master;
DROP TABLE IF EXISTS product_master;
DROP TABLE IF EXISTS order_details;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS etlextractlog;

CREATE TABLE customer_master(
customerid bigint,
name varchar(100) NOT NULL,
address	varchar(254),
city varchar(60),
state varchar(60),
pincode bigint,
update_timestamp timestamp NOT NULL
);

ALTER TABLE customer_master
ADD CONSTRAINT cust_master_PK PRIMARY KEY (customerid);


CREATE TABLE product_master(
productid integer,
productcode varchar(20) NOT NULL,
productname	varchar(100),
sku	varchar(20),
rate numeric(20),
isactive boolean
);

ALTER TABLE product_master
ADD CONSTRAINT prod_master_PK PRIMARY KEY (productid);



CREATE TABLE order_details(			
orderid	bigint,
customerid bigint NOT NULL,
order_status_update_timestamp timestamp,
order_status varchar(50)
);

ALTER TABLE order_details
ADD CONSTRAINT ord_detail_PK PRIMARY KEY (orderid,order_status);

ALTER TABLE order_details
ADD CONSTRAINT ord_detail_FK FOREIGN KEY (customerid) REFERENCES customer_master(customerid);



CREATE TABLE order_items(
orderid	bigint,
productid integer,
quantity integer NOT NULL
);

ALTER TABLE order_items
ADD CONSTRAINT ord_item_PK PRIMARY KEY (orderid,productid);

--ALTER TABLE order_items ADD CONSTRAINT ord_item_FK_ord FOREIGN KEY (orderid) REFERENCES order_details(orderid);

ALTER TABLE order_items
ADD CONSTRAINT ord_item_FK_prod FOREIGN KEY (productid) REFERENCES product_master(productid);

CREATE TABLE etlextractlog(
processlogid integer,
tablename varchar(30),
extractrowcount	bigint,
lastextractdatetime timestamp,
status varchar(20)
);

