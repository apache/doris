# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import random
import string

table_name = "demo.format_v1.sample_parquet"

alphabet = 'abcdefghijklmnopqrstuvwxyz!@#$%^&*()'
binary_alphabet = '11111111111110000000000000000000'
data_choice = ["date('2000-12-31')", "date('1969-09-21')", "date('2969-02-03')"]
timestamp_choice = [    
    "TIMESTAMP '1970-01-01 00:00:01.000001 UTC+00:00'",
    "TIMESTAMP '1970-01-02 00:00:01.000001 UTC+00:00'",
    "TIMESTAMP '1970-01-03 00:00:01.000001 UTC+00:00'",
    "TIMESTAMP '1970-01-04 00:00:01.000001 UTC+00:00'"]
timestamp_ntz_choice = [
    "TIMESTAMP_NTZ '2017-12-01 10:12:55.038194 UTC'",
    "TIMESTAMP_NTZ '2017-12-02 10:12:55.038194 UTC'",
    "TIMESTAMP_NTZ '2017-12-03 10:12:55.038194 UTC'",
    "TIMESTAMP_NTZ '2017-12-04 10:12:55.038194 UTC'",
]
city_choice = [
    "'Shanghai'", "'Hefei'", "'Beijing'", "'Hangzhou'"
]


def get_one_data():
    id = random.randint(-100000000, 100000000)

    col_boolean = True
    if random.randint(-1000000, 1000000) % 2 == 0:
        col_boolean = False

    col_short = random.randint(-32700, 32700)

    col_byte = random.randint(-128, 127)

    col_integer = random.randint(-21474836, 2147483)

    col_long = random.randint(-92233720368547758, 92233720368547758)

    col_float = random.random() * 10

    col_double = random.random() * 10

    col_date = random.choice(data_choice)

    col_timestamp = random.choice(timestamp_choice)

    col_timestamp_ntz = random.choice(timestamp_ntz_choice)

    col_char = "".join(random.sample(alphabet, random.randint(1,18)))

    col_varchar = ''.join(random.sample(string.ascii_letters + string.digits, random.randint(1, 20)))

    col_string = ''.join(random.sample(string.ascii_letters + string.digits, random.randint(1, 20)))

    col_binary = ''.join(random.sample(binary_alphabet, random.randint(1,30)))

    col_decimal = random.random() * 10000

    city = random.choice(city_choice)

    out = "{},{},{},{},{},{},{},{},{},{},{},'{}','{}','{}',CAST('{}' AS BINARY),{},{}".format(
        id,
        col_boolean,
        col_short,
        col_byte,
        col_integer,
        col_long,
        col_float,
        col_double,
        col_date,
        col_timestamp,
        col_timestamp_ntz,
        col_char,
        col_varchar,
        col_string,
        col_binary,
        col_decimal,
        city
    )
    return out

with open('insert_table_values.sql', 'w') as f:
    f.write("INSERT INTO {} VALUES\n".format(table_name))
    f.write("  ({})\n".format(get_one_data()))
    for i in range(1, 1000):
        f.write(", ({})\n".format(get_one_data()))
    f.write(";\n")

