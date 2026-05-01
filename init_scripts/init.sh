#!/bin/bash

set -e

MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}"
MYSQL_USER="${MYSQL_USER}"
MYSQL_PASSWORD="${MYSQL_PASSWORD}"
FILE_NAME="${FILE_NAME:-en.openfoodfacts.org.products.csv}"
DATA_DIR="/var/lib/mysql-files"
FULL_PATH="${DATA_DIR}/${FILE_NAME}"
SELECTED_COLUMNS="${SELECTED_COLUMNS}"

# Проверяем наличие SELECTED_COLUMNS
if [ -z "$SELECTED_COLUMNS" ]; then
    echo "ERROR: SELECTED_COLUMNS environment variable is not set." >&2
    exit 1
fi

# Читаем заголовок из CSV-файла
if [ ! -f "$FULL_PATH" ]; then
    echo "ERROR: CSV file $FULL_PATH not found." >&2
    exit 1
fi

# Получаем заголовок файла
HEADER=$(head -1 "$FULL_PATH")
IFS=$'\t' read -r -a ALL_COLUMNS <<< "$HEADER"

# Создаем массив с безопасными именами колонок (заменяем дефисы на подчеркивания)
SAFE_COLUMNS=()
for col in "${ALL_COLUMNS[@]}"; do
    safe_col=$(echo "$col" | tr '-' '_')
    SAFE_COLUMNS+=("$safe_col")
done

# Обрабатываем выбранные колонки
IFS=' ' read -r -a selected_columns <<< "$SELECTED_COLUMNS"
selected_columns+=("code")
selected_columns=($(printf "%s\n" "${selected_columns[@]}" | sort -u))

# Функция для экранирования имен колонок
escape_column() {
    echo "\`$1\`"
}

# Генерируем SQL для создания таблиц
generate_table_sql() {
    local table_name=$1
    local columns=("${@:2}")
    
    if [ ${#columns[@]} -eq 0 ]; then
        echo "ERROR: No columns defined for table $table_name" >&2
        exit 1
    fi
    
    local sql="CREATE TABLE IF NOT EXISTS ${table_name} ("
    for col in "${columns[@]}"; do
        if [ "$col" == "code" ]; then
            sql+="$(escape_column "$col") VARCHAR(255)"
        else
            sql+="$(escape_column "$col") FLOAT"
        fi
        sql+=", "
    done
    sql="${sql%, });"
    echo "$sql"
}

# Генерируем список колонок для LOAD DATA
generate_load_list() {
    local load_list=""
    for safe_col in "${SAFE_COLUMNS[@]}"; do
        load_list+="@${safe_col}, "
    done
    echo "${load_list%, }"
}

# Генерируем SET-выражения для преобразования данных
generate_set_expr() {
    local set_expr=""
    for col in "${selected_columns[@]}"; do
        # Преобразуем имя колонки для SET (заменяем дефисы на подчеркивания)
        safe_col=$(echo "$col" | tr '-' '_')
        escaped_col=$(escape_column "$col")
        set_expr+="$escaped_col = NULLIF(@${safe_col}, ''), "
    done
    echo "${set_expr%, }"
}

echo "Creating database and user..."
mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" <<EOF
CREATE DATABASE IF NOT EXISTS foodfacts;
DROP USER IF EXISTS '${MYSQL_USER}'@'%';
CREATE USER '${MYSQL_USER}'@'%' IDENTIFIED BY '${MYSQL_PASSWORD}';
GRANT ALL PRIVILEGES ON foodfacts.* TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
EOF

echo "Creating products table..."
PRODUCTS_SQL=$(generate_table_sql "products" "${selected_columns[@]}")
mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" foodfacts <<EOF
$PRODUCTS_SQL
EOF

echo "Importing data..."
LOAD_LIST=$(generate_load_list)
SET_EXPR=$(generate_set_expr)

mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" foodfacts <<EOF
SET SESSION sql_mode = '';
LOAD DATA INFILE '${FULL_PATH}'
INTO TABLE products
FIELDS TERMINATED BY '\t'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
($LOAD_LIST)
SET $SET_EXPR;
EOF

echo "Creating predicts table..."
PREDICT_SQL=$(generate_table_sql "predicts" "${selected_columns[@]}" "prediction")
mysql -h localhost -u root -p"${MYSQL_ROOT_PASSWORD}" foodfacts <<EOF
$PREDICT_SQL
EOF

echo "Initialization completed!"


