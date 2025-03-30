#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import adbc_driver_manager
import adbc_driver_flightsql.dbapi as flight_sql
import pandas
from datetime import datetime

my_uri = "grpc://0.0.0.0:`fe.conf_arrow_flight_port`"
my_db_kwargs = {
    adbc_driver_manager.DatabaseOptions.USERNAME.value: "root",
    adbc_driver_manager.DatabaseOptions.PASSWORD.value: "",
}
sql = "select * from clickbench.hits limit 1000000;"

# PEP 249 (DB-API 2.0) API wrapper for the ADBC Driver Manager.
def dbapi_adbc_execute_fetchallarrow():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start_time = datetime.now()
    cursor.execute(sql)
    arrow_data = cursor.fetchallarrow()
    dataframe = arrow_data.to_pandas()
    print("\n##################\n dbapi_adbc_execute_fetchallarrow" + ", cost:" + str(datetime.now() - start_time) + ", bytes:" + str(arrow_data.nbytes) + ", len(arrow_data):" + str(len(arrow_data)))
    print(dataframe.info(memory_usage='deep'))
    print(dataframe)

# ADBC reads data into pandas dataframe, which is faster than fetchallarrow first and then to_pandas.
def dbapi_adbc_execute_fetch_df():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start_time = datetime.now()
    cursor.execute(sql)
    dataframe = cursor.fetch_df()    
    print("\n##################\n dbapi_adbc_execute_fetch_df" + ", cost:" + str(datetime.now() - start_time))
    print(dataframe.info(memory_usage='deep'))
    print(dataframe)

# Can read multiple partitions in parallel.
def dbapi_adbc_execute_partitions():
    conn = flight_sql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    cursor = conn.cursor()
    start_time = datetime.now()
    partitions, schema = cursor.adbc_execute_partitions(sql)
    cursor.adbc_read_partition(partitions[0])
    arrow_data = cursor.fetchallarrow()
    dataframe = arrow_data.to_pandas()
    print("\n##################\n dbapi_adbc_execute_partitions" + ", cost:" + str(datetime.now() - start_time) + ", len(partitions):" + str(len(partitions)))
    print(dataframe.info(memory_usage='deep'))
    print(dataframe)

import adbc_driver_flightsql
import pyarrow

# ADBC Low-level api is root module, provides a fairly direct, 1:1 mapping to the C API definitions in Python. 
# For a higher-level interface, use adbc_driver_manager.dbapi. (This requires PyArrow.)
def low_level_api_execute_query():
    db = adbc_driver_flightsql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    conn = adbc_driver_manager.AdbcConnection(db)
    stmt = adbc_driver_manager.AdbcStatement(conn)
    stmt.set_sql_query(sql)
    start_time = datetime.now()
    stream, rows = stmt.execute_query()
    reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
    arrow_data = reader.read_all()
    dataframe = arrow_data.to_pandas()
    print("\n##################\n low_level_api_execute_query" + ", cost:" + str(datetime.now() - start_time) + ", stream.address:" + str(stream.address) + ", rows:" + str(rows) + ", bytes:" + str(arrow_data.nbytes) + ", len(arrow_data):" + str(len(arrow_data)))
    print(dataframe.info(memory_usage='deep'))
    print(dataframe)

# Can read multiple partitions in parallel.
def low_level_api_execute_partitions():
    db = adbc_driver_flightsql.connect(uri=my_uri, db_kwargs=my_db_kwargs)
    conn = adbc_driver_manager.AdbcConnection(db)
    stmt = adbc_driver_manager.AdbcStatement(conn)
    stmt.set_sql_query(sql)
    start_time = datetime.now()
    streams = stmt.execute_partitions()
    for s in streams[0]:
        stream = conn.read_partition(s)
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        arrow_data = reader.read_all()
        dataframe = arrow_data.to_pandas()
    print("\n##################\n low_level_api_execute_partitions" + ", cost:" + str(datetime.now() - start_time) + "streams.size:" + str(len(streams)) + ", "  + str(len(streams[0])) + ", " + str(streams[2]))

dbapi_adbc_execute_fetchallarrow()
dbapi_adbc_execute_fetch_df()
dbapi_adbc_execute_partitions()
low_level_api_execute_query()
low_level_api_execute_partitions()
