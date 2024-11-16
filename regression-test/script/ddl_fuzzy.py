import random
from typing import Optional
import re
from time import sleep
import mysql.connector
from mysql.connector import errorcode

DATA_TYPES_BASE = ["tinyint","smallint","int","bigint","largeint","float"]
DATA_TYPES_CHAR = ["varchar(255)"]

# todo using map<pair> to save partition
partition_start = 0
partition_end = 2

def parse_jdbc_url(jdbc_url):
    match = re.match(r"jdbc:mysql://([^:/]+):(\d+)/([^?]+)", jdbc_url)
    if not match:
        raise ValueError("Invalid JDBC URL format.")
    
    host, port, database = match.groups()
    return host, port, database

def get_columns(cursor, table):
    cursor.execute(f"DESCRIBE {table};")
    columns_info = cursor.fetchall()
    
    columns = [row[0] for row in columns_info]
    column_types = {row[0]: row[1] for row in columns_info}
    
    return columns, column_types

def get_partition(cursor, table):
    cursor.execute(f"SHOW PARTITIONS from {table};")
    partition_info = cursor.fetchall()
    
    partition = [row[1] for row in partition_info]
    
    return partition

def generate_random_ddl_clauses(table, existing_columns, column_types, partitions):

    global partition_start
    global partition_end
    ddl_clauses = []
    # , "rename_column",
    if(partitions):
        ddl_type = random.choice(["modify_column", "add_column" "drop_column" ,"drop_partition", "add_partition"])
    else:
        ddl_type = "add_partition"

    if ddl_type == "add_column":
        column_name = f"col_{random.randint(partition_start,partition_end)}"
        data_type = random.choice(DATA_TYPES_BASE)
        ddl_clauses = f"ALTER TABLE {table} ADD COLUMN {column_name} {data_type};"
    
    elif ddl_type == "rename_column" and existing_columns:
        old_column = random.choice(existing_columns)
        new_column = f"renamed_col_{random.randint(1, 100)}"
        ddl_clauses = f"ALTER TABLE {table} RENAME COLUMN {old_column} {new_column};"
        existing_columns.remove(old_column)
        existing_columns.append(new_column)
    
    elif ddl_type == "modify_column" and existing_columns:
        column_name = random.choice(existing_columns)
        if DATA_TYPES_BASE.count(column_types[column_name]):
            new_data_type = random.choice(DATA_TYPES_CHAR)
        else:
            new_data_type = random.choice(DATA_TYPES_BASE)

        ddl_clauses = f"ALTER TABLE {table} MODIFY COLUMN {column_name} {new_data_type};"
    
    elif ddl_type == "drop_column" and existing_columns and len(column_types) > 1 and partitions:
        column_name = random.choice(existing_columns)
        ddl_clauses = f"ALTER TABLE {table} DROP COLUMN {column_name};"
        existing_columns.remove(column_name)
    
    elif ddl_type == "add_partition":
        if not partitions:

            partition_start = 0
            partition_end = 2
            partition_name = f"p_{partition_start}_{partition_end}"
        else:
            last_partition_name = partitions[-1]
            last_value_range1, last_value_range2 = map(int, last_partition_name.split('_')[1:])
            new_start = last_value_range2 + 1
            new_end = new_start + 2
            partition_start = new_start
            partition_end = new_end
            partition_name = f"p_{new_start}_{new_end}"

        partitions.append(partition_name)
        value_range1, value_range2 = map(int, partition_name.split('_')[1:])

        ddl_clauses = f"ALTER TABLE {table} ADD PARTITION {partition_name} VALUES [({value_range1}), ({value_range2}));"

    elif ddl_type == "drop_partition":
        if partitions:
            partition_name = random.choice(partitions)
            partitions.remove(partition_name)
            partition_start -= 2
            partition_end -= 2
            if(partition_start < 0):
                partition_start = 0
                partition_end = 2
            ddl_clauses = f"ALTER TABLE {table} DROP PARTITION {partition_name};"

    return ddl_clauses

def execute_ddl(jdbc_url, db_name=None, table_name=None):
    host, port, database = parse_jdbc_url(jdbc_url)
    db_name = db_name or database
    ddl_executions = []
    
    conn = None
    cursor = None
    
    try:
        conn = mysql.connector.connect(
            host=host,
            port=port,
            database=db_name,
            user="root",
            password=""
        )
        cursor = conn.cursor()
        
        if table_name:
            tables = [table_name]
        else:
            cursor.execute("SHOW TABLES;")
            tables = [row[0] for row in cursor.fetchall()]
        
        for table in tables:
            num_clauses = random.randint(1, 6)
            for _ in range(num_clauses):
                existing_columns, column_types = get_columns(cursor, table)
                partitions = get_partition(cursor, table)

                if not existing_columns:
                    print(f"No columns found in table {table}. Skipping.")
                    continue

                # ddl
                ddl_statement = generate_random_ddl_clauses(table, existing_columns, column_types, partitions)
                if ddl_statement:
                    print(f"Executing DDL: {ddl_statement}")
                    ddl_executions.append(ddl_statement)
                    cursor.execute(ddl_statement)
                    while 1:
                        show_column = f"SHOW ALTER TABLE COLUMN WHERE TableName = \"{table}\" ORDER BY CreateTime DESC LIMIT 1;"
                        cursor.execute(show_column)
                        status = cursor.fetchall()
                        if(not status):
                            break
                        if status and status[0][9] == 'FINISHED':
                            break

                    existing_columns, column_types = get_columns(cursor, table)
                
                # delete
                delete_statement = generate_random_delete(table, existing_columns,partitions)
                if delete_statement:
                    print(f"Executing DELETE: {delete_statement}")
                    ddl_executions.append(delete_statement)
                    cursor.execute(delete_statement)
                    existing_columns, column_types = get_columns(cursor, table)

                # insert
                insert_statement = generate_random_insert(table, existing_columns, column_types,partitions)
                if insert_statement:
                    print(f"Executing INSERT: {insert_statement}")
                    ddl_executions.append(insert_statement)
                    cursor.execute(insert_statement)
            conn.commit()
            print("DDL and DML execution completed successfully.")
        
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist.")
        else:
            print(err)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return ddl_executions

def generate_random_delete(table, existing_columns,partitions):
    if not existing_columns:
        return None
    
    column_name = random.choice(existing_columns)
    
    operators = ['==', '>', '<', '>=', '<=', '!=']
    
    operator = random.choice(operators)
    
    value = random.randint(partition_start, partition_end)

    condition = f"{column_name} {operator} {value}"

    if(not partitions):
        delete_statement = f"DELETE FROM {table} WHERE {condition};"
    else:
        partition = random.choice(partitions)
        delete_statement = f"DELETE FROM {table} PARTITION {partition} where {condition};"
    
    return delete_statement

def generate_random_insert(table, columns, column_types,partitions):
    if not columns:
        return None

    values = []
    if(partitions):
        last_partition_name = random.choice(partitions)
        global partition_start
        global partition_end
        last_value_range1, last_value_range2 = map(int, last_partition_name.split('_')[1:])
        print(last_value_range1,last_value_range2)
    else:
        last_value_range1 = 0
        last_value_range2 = 2
    for col in columns:
        col_type = column_types[col].upper()
        if "INT" in col_type:
            values.append(str(random.randint(last_value_range1, last_value_range2)))
        elif "VARCHAR" in col_type:
            values.append(f"'{random.randint(last_value_range1, last_value_range2)}'")
        elif "DATE" in col_type:
            values.append(f"'{random.randint(1, 28):02d}'")
        else:
            values.append("NULL")
    
    insert_statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)});"
    return insert_statement

# Example usage
if __name__ == "__main__":
    jdbc_url = "jdbc:mysql://127.0.0.1:9030/"
    db_name = ""  # Set to None to use the default from the JDBC URL
    table_name = ""  # Set to None to apply to random tables in the database
    
    while 1: 
        execute_ddl(jdbc_url, db_name, table_name)
        sleep(1)

    print("Generated DDL and DML Statements:")