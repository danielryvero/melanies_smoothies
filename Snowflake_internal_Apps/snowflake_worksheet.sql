---create Database to work with
CREATE DATABASE SMOOTHIES;

---table for smoothies
CREATE TABLE FRUIT_OPTIONS (
FRUIT_ID NUMERIC,
FRUIT_NAME VARCHAR(25)
);

---file format to load the file
create file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

---now you should create a SNOWFLAKE MANAGED STAGE in the PUBLIC 
---schema of the database. Call it MY_UPLOADED_FILES.
---Upload the content of the "fruits_available_for_smoothies.txt" 
---file in the previous folder to it

---query data but still not load it
select $1, $2, $3, $4, $5 
from @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt
(file_format => TWO_HEADERROW_PCT_DELIM);

---try to load and understand the errors
COPY INTO smoothies.public.fruit_options
from @smoothies.public.my_uploaded_files
files = ('fruits_available_for_smoothies.txt')
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = abort_statement
validation_mode = return_errors
purge = true;

---copy statement
COPY INTO SMOOTHIES.PUBLIC.FRUIT_OPTIONS 
FROM (
select $2 as fruit_id, $1 as fruit_name
from @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt)
file_format = (format_name = SMOOTHIES.PUBLIC.TWO_HEADERROW_PCT_DELIM)
on_error = abort_statement
purge = true
;

---create a sequence, you will need it to insert data later
CREATE OR REPLACE SEQUENCE ORDER_SEQ 
START = 1 INCREMENT = 1;

---check data in table
select *
from SMOOTHIES.PUBLIC.FRUIT_OPTIONS;

---create table for orders
CREATE OR REPLACE TABLE ORDERS (
order_filled BOOLEAN DEFAULT FALSE,
name_on_order VARCHAR(100),
ingredients VARCHAR(200),
order_uid INTEGER DEFAULT SMOOTHIES.PUBLIC.ORDER_SEQ.NEXTVAL,
CONSTRAINT order_uid UNIQUE (order_uid) ENFORCED,
order_ts timestamp_ltz default current_timestamp()
);


---insert some things into orders
INSERT INTO ORDERS (ingredients, name_on_order, order_filled) values
('guava', 'Max', false),
('grapes', 'Larry', true),
('apple', 'Daniel', false)
;

---check data in orders
select *
from orders;


---add search_on to fruit_options table to integrate with API
alter table fruit_options add column search_on VARCHAR(25);

---now search_on is equal to fruit names
update fruit_options 
set search_on = fruit_name;

select *
from fruit_options;

---now search on must match the APi names, so change names on search_on
update fruit_options
set search_on = 'Raspberry'
where fruit_id = 17;

---when using fruityvice.com, the names to change are:
--- (id, fruit name on page) (1, Apple) (2, Blueberry) (4, Dragonfruit) (6, Fig)
---                          (17, Raspberry) (18, Strawberry)
select *
from fruit_options 
where fruit_name not like search_on;
