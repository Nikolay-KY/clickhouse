CREATE TABLE IF NOT EXISTS kazkakov.winemag (
line_id UInt32,
country String,
description String,
designation String, 
points UInt8,
price Decimal32(3),
province String,
region_1 String,
region_2 String,
taster_name String,
taster_twitter_handle String,
title String,
variety String,
winery String
) Engine = MergeTree()
order by line_id;

-- cd ~/Загрузки ; clickhouse-client -h 127.0.0.1 -u default --format_csv_delimiter="," --query "insert into kazakov.winemag FORMAT CSV" < winemag-data-130k-v2.csv

-- найти максимальную цену для каждой страны

select 
	country, 
	max(price) as max_price
from kazakov.winemag w 
where country <> '' and price > 0.0
group by country
order by max_price desc;

-- вывести топ-10 стран с самыми дорогими винами (country, max_price)
with max_price as (
select 
	country, 
	max(price) as max_price
from kazakov.winemag w 
where country <> '' and price > 0.0
group by country
order by max_price desc
limit 10)

select 
	country, 
	max(price) as max_price
from kazakov.winemag w 
where country <> '' and price > 0.0
group by country
HAVING country in (select country from max_price)
order by max_price desc
