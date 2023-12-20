CREATE database if not exists kazakovwine;

use kazakovwine;

CREATE table wine(
id UInt32,
country String,
description String,
designation String,
points UInt8,
price Float32,
province String,
region_1 String,
region_2 String,
taster_name String,
taster_twitter_handle String,
title String,
variety String,
winery String
) Engine=MergeTree()
order by id;

-- run in terminal
-- clickhouse-client -h 127.0.0.1 -u default --pass qw123456 --format_csv_delimiter=',' --input_format_csv_skip_first_lines=1 --query "insert into kazakovwine.wine format CSV" < winemag-data-130k-v2.csv
---- оставить только непустые значения для названий стран и цен
SELECT country, price
FROM wine
WHERE country IS NOT NULL AND price IS NOT NULL;

---- найти максимальную цену для каждой страны
SELECT country, MAX(price) AS max_price
FROM wine
WHERE country IS NOT NULL AND price IS NOT NULL
GROUP BY country;

-- вывести топ-10 стран с самыми дорогими винами (country, max_price)
SELECT country, MAX(price) AS max_price
FROM wine
WHERE country IS NOT NULL AND price IS NOT NULL
GROUP BY country
ORDER BY max_price DESC
LIMIT 10;

--определить как высокая цена коррелирует с оценкой дегустатора (насколько дорогие вина хорошие)
SELECT corr(price, points) AS correlation
FROM wine
WHERE price IS NOT NULL AND points IS NOT NULL;

--учесть в выборке также регион производства

SELECT region_1, corr(price, points) AS correlation
FROM wine
WHERE price IS NOT NULL AND points IS NOT NULL
GROUP BY region_1;
