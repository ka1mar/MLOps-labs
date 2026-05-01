#!/bin/bash

set -e

# Load configuration from .env file
if [ -f "$(dirname "$0")/../.env" ]; then
    source "$(dirname "$0")/../.env"
    echo "Loaded configuration from .env file"
else
    echo "Error: .env file not found!"
    exit 1
fi

echo "Creating database and user..."
kubectl exec deployment/mysql -n foodfacts -- mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "
CREATE DATABASE IF NOT EXISTS foodfacts;
CREATE USER IF NOT EXISTS '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON foodfacts.* TO '$MYSQL_USER'@'%';
FLUSH PRIVILEGES;"

# Wait for data file to be available in the MySQL container
echo "Checking if CSV data file is available..."
while ! kubectl exec deployment/mysql -n foodfacts -- test -f "/var/lib/mysql-files/$FILE_NAME"; do
  echo "Waiting for CSV data file..."
  sleep 5
done

echo "Creating tables..."
kubectl exec deployment/mysql -n foodfacts -- mysql -u root -p"$MYSQL_ROOT_PASSWORD" foodfacts -e "
CREATE TABLE IF NOT EXISTS products (
  code VARCHAR(255),
  energy_100g FLOAT,
  proteins_100g FLOAT,
  carbohydrates_100g FLOAT,
  fat_100g FLOAT,
  energy_kcal_100g FLOAT,
  sugars_100g FLOAT,
  saturated_fat_100g FLOAT,
  salt_100g FLOAT,
  sodium_100g FLOAT
);

CREATE TABLE IF NOT EXISTS predicts (
  code VARCHAR(255),
  energy_100g FLOAT,
  proteins_100g FLOAT,
  carbohydrates_100g FLOAT,
  fat_100g FLOAT,
  energy_kcal_100g FLOAT,
  sugars_100g FLOAT,
  saturated_fat_100g FLOAT,
  salt_100g FLOAT,
  sodium_100g FLOAT,
  prediction FLOAT
);"

echo "Loading data from CSV..."
kubectl exec deployment/mysql -n foodfacts -- mysql -u root -p"$MYSQL_ROOT_PASSWORD" foodfacts -e "
SET SESSION sql_mode = '';
LOAD DATA INFILE '/var/lib/mysql-files/$FILE_NAME'
INTO TABLE products
FIELDS TERMINATED BY '\t'
ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@col1,@col2,@col3,@col4,@col5,@col6,@col7,@col8,@col9,@col10,@col11,@col12,@col13,@col14,@col15,@col16,@col17,@col18,@col19,@col20,@col21,@col22,@col23,@col24,@col25,@col26,@col27,@col28,@col29,@col30,@col31,@col32,@col33,@col34,@col35,@col36,@col37,@col38,@col39,@col40,@col41,@col42,@col43,@col44,@col45,@col46,@col47,@col48,@col49,@col50,@col51,@col52,@col53,@col54,@col55,@col56,@col57,@col58,@col59,@col60,@col61,@col62,@col63,@col64,@col65,@col66,@col67,@col68,@col69,@col70,@col71,@col72,@col73,@col74,@col75,@col76,@col77,@col78,@col79,@col80,@col81,@col82,@col83,@col84,@col85,@col86,@col87,@col88,@col89,@col90,@col91,@col92,@col93,@col94,@col95,@col96,@col97,@col98,@col99,@col100,@col101,@col102,@col103,@col104,@col105,@col106,@col107,@col108,@col109,@col110,@col111,@col112,@col113,@col114,@col115,@col116,@col117,@col118,@col119,@col120,@col121,@col122,@col123,@col124,@col125,@col126,@col127,@col128,@col129,@col130,@col131,@col132,@col133,@col134,@col135,@col136,@col137,@col138,@col139,@col140,@col141,@col142,@col143,@col144,@col145,@col146,@col147,@col148,@col149,@col150,@col151,@col152,@col153,@col154,@col155,@col156,@col157,@col158,@col159,@col160,@col161,@col162,@col163,@col164,@col165,@col166,@col167,@col168,@col169,@col170,@col171,@col172,@col173,@col174,@col175,@col176,@col177,@col178,@col179,@col180,@col181,@col182,@col183,@col184,@col185,@col186,@col187,@col188,@col189,@col190,@col191,@col192,@col193,@col194,@col195,@col196,@col197,@col198,@col199,@col200,@col201,@col202,@col203,@col204,@col205,@col206,@col207,@col208,@col209,@col210,@col211,@col212,@col213,@col214)
SET 
  code = NULLIF(@col1, ''),
  energy_100g = NULLIF(@col91, ''),
  proteins_100g = NULLIF(@col150, ''),
  carbohydrates_100g = NULLIF(@col130, ''),
  fat_100g = NULLIF(@col93, ''),
  energy_kcal_100g = NULLIF(@col90, ''),
  sugars_100g = NULLIF(@col131, ''),
  saturated_fat_100g = NULLIF(@col94, ''),
  salt_100g = NULLIF(@col154, ''),
  sodium_100g = NULLIF(@col156, '');"

echo "Database initialization completed!"
