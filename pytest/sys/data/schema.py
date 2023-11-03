#!/bin/env python
# -*- coding: utf-8 -*-
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

"""
the data set for test load on palo
Date: 2018-07-10 15:12:35
"""
import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../lib")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from palo_client import PartitionInfo
from palo_client import DistributionInfo

baseall_column_list = [("k1", "tinyint"),
                       ("k2", "smallint"),
                       ("k3", "int"),
                       ("k4", "bigint"),
                       ("k5", "decimal(9, 3)"),
                       ("k6", "char(5)"),
                       ("k10", "date"),
                       ("k11", "datetime"),
                       ("k7", "varchar(20)"),
                       ("k8", "double", "max"),
                       ("k9", "float", "sum")]

baseall_column_no_agg_list = [("k1", "tinyint"),
                              ("k2", "smallint"),
                              ("k3", "int"),
                              ("k4", "bigint"),
                              ("k5", "decimal(9, 3)"),
                              ("k6", "char(5)"),
                              ("k10", "date"),
                              ("k11", "datetime"),
                              ("k7", "varchar(20)"),
                              ("k8", "double"),
                              ("k9", "float")]

baseall_column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k10', 'k11', 'k7', 'k8', 'k9']

baseall_tinyint_partition_info = PartitionInfo("k1",
                                       ["p1", "p2", "p3", "p4", "p5"],
                                       ["-10", "0", "10", "100", "MAXVALUE"])

baseall_smallint_partition_info = PartitionInfo(
    'k2',
    ['partition_a', 'partition_b', 'partition_c', 'partition_d'],
    ['0', '5', '10', 'MAXVALUE']
)

baseall_int_partition_info = PartitionInfo(
    'k3',
    ['partition_a', 'partition_b', 'partition_c', 'partition_d'],
    ['0', '2000', '65536', 'MAXVALUE']
)

baseall_bigint_partition_info = PartitionInfo(
    'k4',
    ['partition_a', 'partition_b', 'partition_c', 'partition_d'],
    ['-11011905', '0', '11011905', 'MAXVALUE']
)

baseall_date_partition_info = PartitionInfo(
    'k10',
    ['partition_a', 'partition_b', 'partition_c'],
    ['1910-01-01', '2012-03-14', 'MAXVALUE']
)

baseall_datetime_partition_info = PartitionInfo(
    'k11',
    ['partition_a', 'partition_b', 'partition_c'],
    ['1910-01-01 00:00:00', '2012-03-14 00:00:00', 'MAXVALUE']
)

baseall_distribution_info = DistributionInfo('HASH(k1)', 5)
baseall_duplicate_key = 'DUPLICATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7)' 
baseall_unique_key = 'UNIQUE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7)'

all_type_column_list = [("tinyint_key", "tinyint"), ("smallint_key", "smallint"),
        ("int_key", "int"), ("bigint_key", "bigint"),
        ("char_50_key", "char(50)"), ("varchar_key", "varchar(500)"),
        ("char_key", "char"), ("varchar_most_key", "varchar(65533)"),
        ("decimal_key", "decimal(20, 6)"), ("decimal_most_key", "decimal(27, 9)"),
        ("date_key", "date"), ("datetime_key", "datetime"),
        ("tinyint_value", "tinyint", "sum"), ("smallint_value", "smallint", "sum"),
        ("int_value", "int", "sum"), ("bigint_value", "bigint", "sum"),
        ("char_50_value", "char(50)", "replace"),
        ("varchar_value", "varchar(500)", "replace"),
        ("char_value", "char", "replace"),
        ("varchar_most_value", "varchar(65533)", "replace"),
        ("decimal_value", "decimal(20, 6)", "sum"),
        ("decimal_most_value", "decimal(27, 9)", "sum"),
        ("date_value_max", "date", "max"),
        ("date_value_replace", "date", "replace"),
        ("date_value_min", "date", "min"),
        ("datetime_value_replace", "datetime", "replace"),
        ("datetime_value_max", "datetime", "replace"),
        ("datetime_value_min", "datetime", "replace"),
        ("float_value", "float", "sum"),
        ("double_value", "double", "sum")
    ]

all_type_column_name_list = ['tinyint_key', 'smallint_key', 'int_key', 'bigint_key',
                             'char_50_key', 'varchar_key', 'char_key', 'varchar_most_key',
                             'decimal_key', 'deciaml_most_key', 'date_key', 'datetime_key']

partition_column_list = [('k1', 'TINYINT'),
            ('k2', 'SMALLINT'),
            ('k3', 'INT'),
            ('k4', 'BIGINT'),
            ('k5', 'DATETIME'),
            ('v1', 'DATE', 'REPLACE'),
            ('v2', 'CHAR', 'REPLACE'),
            ('v3', 'VARCHAR(4096)', 'REPLACE'),
            ('v4', 'FLOAT', 'SUM'),
            ('v5', 'DOUBLE', 'SUM'),
            ('v6', 'DECIMAL(20,7)', 'SUM')]

partition_column_list_parse = [('k1', 'TINYINT'),
            ('k2', 'SMALLINT'),
            ('k3', 'INT'),
            ('k4', 'BIGINT'),
            ('k5_t', 'DATETIME'),
            ('v1', 'DATE', 'REPLACE'),
            ('v2', 'CHAR', 'REPLACE'),
            ('v3', 'VARCHAR(4096)', 'REPLACE'),
            ('v4', 'FLOAT', 'SUM'),
            ('v5', 'DOUBLE', 'SUM'),
            ('v6', 'DECIMAL(20,7)', 'SUM')]

partition_column_no_agg_list = [('k1', 'TINYINT'),
            ('k2', 'SMALLINT'),
            ('k3', 'INT'),
            ('k4', 'BIGINT'),
            ('k5', 'DATETIME'),
            ('v1', 'DATE'),
            ('v2', 'CHAR'),
            ('v3', 'VARCHAR(4096)'),
            ('v4', 'FLOAT'),
            ('v5', 'DOUBLE'),
            ('v6', 'DECIMAL(20,7)')]

partition_column_name_list = ['k1', 'k2', 'k3', 'k4', 'k5', 'v1', 'v2', 'v3', 'v4', 'v5', 'v6']

tinyint_column_list = [("k1", "TINYINT"),
        ("v1", "TINYINT", "SUM"),
        ("v2", "TINYINT", "MAX"),
        ("v3", "TINYINT", "MIN"),
        ("v4", "TINYINT", "REPLACE")]

tinyint_column_no_agg_list = [("k1", "TINYINT"),
                              ("v1", "TINYINT"), 
                              ("v2", "TINYINT"),
                              ("v3", "TINYINT"),
                              ("v4", "TINYINT")]

smallint_column_list = [("k1", "SMALLINT"),
        ("v1", "SMALLINT", "SUM"),
        ("v2", "SMALLINT", "MAX"),
        ("v3", "SMALLINT", "MIN"),
        ("v4", "SMALLINT", "REPLACE")]

smallint_column_no_agg_list = [("k1", "SMALLINT"),
                               ("v1", "SMALLINT"),
                               ("v2", "SMALLINT"),
                               ("v3", "SMALLINT"),
                               ("v4", "SMALLINT")]

int_column_list = [("k1", "INT"),
        ("v1", "INT", "SUM"),
        ("v2", "INT", "MAX"),
        ("v3", "INT", "MIN"),
        ("v4", "INT", "REPLACE")]

int_column_no_agg_list = [("k1", "INT"),
                              ("v1", "INT"),
                              ("v2", "INT"),
                              ("v3", "INT"),
                              ("v4", "INT")]

bigint_column_list = [("k1", "BIGINT"),
        ("v1", "BIGINT", "SUM"),
        ("v2", "BIGINT", "MAX"),
        ("v3", "BIGINT", "MIN"),
        ("v4", "BIGINT", "REPLACE")]

bigint_column_no_agg_list = [("k1", "BIGINT"),
                      ("v1", "BIGINT"),
                      ("v2", "BIGINT"),
                      ("v3", "BIGINT"),
                      ("v4", "BIGINT")]

largeint_column_list = [("k1", "LARGEINT"),
        ("v1", "LARGEINT", "SUM"),
        ("v2", "LARGEINT", "MAX"),
        ("v3", "LARGEINT", "MIN"),
        ("v4", "LARGEINT", "REPLACE")]

largeint_column_no_agg_list = [("k1", "LARGEINT"),
                        ("v1", "LARGEINT"),
                        ("v2", "LARGEINT"),
                        ("v3", "LARGEINT"),
                        ("v4", "LARGEINT")]

float_column_list = [("k1", "INT"),
        ("v1", "FLOAT", "SUM"),
        ("v2", "FLOAT", "MAX"),
        ("v3", "FLOAT", "MIN"),
        ("v4", "FLOAT", "REPLACE")]

float_column_no_agg_list = [("k1", "INT"),
        ("v1", "FLOAT"),
        ("v2", "FLOAT"),
        ("v3", "FLOAT"),
        ("v4", "FLOAT")]

double_column_list = [("k1", "INT"),
        ("v1", "DOUBLE", "SUM"),
        ("v2", "DOUBLE", "MAX"),
        ("v3", "DOUBLE", "MIN"),
        ("v4", "DOUBLE", "REPLACE")]

double_column_no_agg_list = [("k1", "INT"),
        ("v1", "DOUBLE"),
        ("v2", "DOUBLE"),
        ("v3", "DOUBLE"),
        ("v4", "DOUBLE")]

decimal_least_column_list = [("k1", "DECIMAL(1, 0)"),
        ("v1", "DECIMAL(1, 0)", "SUM"),
        ("v2", "DECIMAL(1, 0)", "MAX"),
        ("v3", "DECIMAL(1, 0)", "MIN"),
        ("v4", "DECIMAL(1, 0)", "REPLACE")]

decimal_least_column_no_agg_list = [("k1", "DECIMAL(1, 0)"),
                             ("v1", "DECIMAL(1, 0)"),
                             ("v2", "DECIMAL(1, 0)"),
                             ("v3", "DECIMAL(1, 0)"),
                             ("v4", "DECIMAL(1, 0)")]

decimal_normal_column_list = [("k1", "DECIMAL(10, 5)"),
        ("v1", "DECIMAL(10, 5)", "SUM"),
        ("v2", "DECIMAL(10, 5)", "MAX"),
        ("v3", "DECIMAL(10, 5)", "MIN"),
        ("v4", "DECIMAL(10, 5)", "REPLACE")]

decimal_normal_column_no_agg_list = [("k1", "DECIMAL(10, 5)"),
                              ("v1", "DECIMAL(10, 5)"),
                              ("v2", "DECIMAL(10, 5)"),
                              ("v3", "DECIMAL(10, 5)"),
                              ("v4", "DECIMAL(10, 5)")]

decimal_most_column_list = [("k1", "DECIMAL(27, 9)"),
        ("v1", "DECIMAL(27, 9)", "SUM"),
        ("v2", "DECIMAL(27, 9)", "MAX"),
        ("v3", "DECIMAL(27, 9)", "MIN"),
        ("v4", "DECIMAL(27, 9)", "REPLACE")]

decimal_most_column_no_agg_list = [("k1", "DECIMAL(27, 9)"),
                            ("v1", "DECIMAL(27, 9)"),
                            ("v2", "DECIMAL(27, 9)"),
                            ("v3", "DECIMAL(27, 9)"),
                            ("v4", "DECIMAL(27, 9)")]

date_column_list = [("k1", "DATE"),
        ("v1", "DATE", "REPLACE"),
        ("v2", "DATE", "MAX"),
        ("v3", "DATE", "MIN")]

date_column_no_agg_list = [("k1", "DATE"),
                    ("v1", "DATE"),
                    ("v2", "DATE"),
                    ("v3", "DATE")]

datetime_column_list = [("k1", "DATETIME"),
        ("v1", "DATETIME", "REPLACE"),
        ("v2", "DATETIME", "MAX"),
        ("v3", "DATETIME", "MIN")]

datetime_column_no_agg_list = [("k1", "DATETIME"),
                        ("v1", "DATETIME"),
                        ("v2", "DATETIME"),
                        ("v3", "DATETIME")]

char_least_column_list = [("k1", "CHAR"),
        ("v1", "CHAR", "REPLACE")]

char_least_column_no_agg_list = [("k1", "CHAR"),
        ("v1", "CHAR")]

char_normal_column_list = [("k1", "CHAR(20)"),
        ("v1", "CHAR(20)", "REPLACE")]

char_normal_column_no_agg_list = [("k1", "CHAR(20)"),
        ("v1", "CHAR(20)")]

char_most_column_list = [("k1", "CHAR(255)"),
        ("v1", "CHAR(255)", "REPLACE")]

char_most_column_no_agg_list = [("k1", "CHAR(255)"),
        ("v1", "CHAR(255)")]

varchar_least_column_list = [("k1", "VARCHAR(1)"),
        ("v1", "VARCHAR(1)", "REPLACE")]

varchar_least_column_no_agg_list = [("k1", "VARCHAR(1)"),
        ("v1", "VARCHAR(1)")]

varchar_normal_column_list = [("k1", "VARCHAR(200)"),
        ("v1", "VARCHAR(200)", "REPLACE")]

varchar_normal_column_no_agg_list = [("k1", "VARCHAR(200)"),
        ("v1", "VARCHAR(200)")]

varchar_most_column_list = [("k1", "VARCHAR(65533)"),
        ("v1", "VARCHAR(65533)", "REPLACE")]

varchar_most_column_no_agg_list = [("k1", "VARCHAR(65533)"),
        ("v1", "VARCHAR(65533)")]

string_column_list = [("k1", "VARCHAR(65535)"),
                      ("v1", "STRING", "REPLACE"),
                      ("v2", "STRING", "MAX"),
                      ("v3", "STRING", "MIN")]

string_column_no_agg_list = [("k1", "VARCHAR(65535)"),
                             ("v1", "STRING")]

json_column_no_agg_list = [("k1", "INT"),
                           ("k2", "CHAR(20)"),
                           ("k3", "double")]

hll_tinyint_column_list = [('k1', 'TINYINT'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_smallint_column_list = [('k1', 'SMALLINT'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_int_column_list = [('k1', 'INT'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_bigint_column_list = [('k1', 'BIGINT'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_largeint_column_list = [('k1', 'LARGEINT'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_char_column_list = [('k1', 'CHAR(20)'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_varchar_column_list = [('k1', 'VARCHAR(1)'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_date_column_list = [('k1', 'DATE'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_datetime_column_list = [('k1', 'DATETIME'), ('v1', 'HLL', 'HLL_UNION'), ('v2', 'int', 'SUM')]
hll_decimal_column_list = [('k1', 'DECIMAL(10, 5)'), ('v1', 'HLL', 'HLL_UNION'), 
                           ('v2', 'int', 'SUM')]

bitmap_int_column_list = [('k1', 'INT'), ('v1', 'BITMAP', 'BITMAP_UNION'), ('v2', 'int', 'SUM')]

hash_distribution_info = DistributionInfo("HASH(k1)", 15)
aggregate_key = "AGGREGATE KEY(k1)"
duplicate_key = "DUPLICATE KEY(k1)"
unique_key = "UNIQUE KEY(k1)"

types_kv_column_list = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'LARGEINT'), \
            ('k6', 'DATE'), \
            ('k7', 'DATETIME'), \
            ('k8', 'CHAR(3)'), \
            ('k9', 'VARCHAR(10)'), \
            ('k10', 'DECIMAL(5,3)'), \
            ('v1', 'TINYINT', 'SUM'), \
            ('v2', 'SMALLINT', 'SUM'), \
            ('v3', 'INT', 'SUM'), \
            ('v4', 'BIGINT', 'SUM'), \
            ('v5', 'LARGEINT', 'SUM'), \
            ('v6', 'DATETIME', 'REPLACE'), \
            ('v7', 'DATE', 'REPLACE'), \
            ('v8', 'CHAR(10)', 'REPLACE'), \
            ('v9', 'VARCHAR(6)', 'REPLACE'), \
            ('v10', 'DECIMAL(27,9)', 'SUM')]

types_kv_column_no_agg_list = [('k1', 'TINYINT'), \
            ('k2', 'SMALLINT'), \
            ('k3', 'INT'), \
            ('k4', 'BIGINT'), \
            ('k5', 'LARGEINT'), \
            ('k6', 'DATE'), \
            ('k7', 'DATETIME'), \
            ('k8', 'CHAR(3)'), \
            ('k9', 'VARCHAR(10)'), \
            ('k10', 'DECIMAL(5,3)'), \
            ('v1', 'TINYINT'), \
            ('v2', 'SMALLINT'), \
            ('v3', 'INT'), \
            ('v4', 'BIGINT'), \
            ('v5', 'LARGEINT'), \
            ('v6', 'DATETIME'), \
            ('v7', 'DATE'), \
            ('v8', 'CHAR(10)'), \
            ('v9', 'VARCHAR(6)'), \
            ('v10', 'DECIMAL(27,9)')]
types_kv_aggregate_key = "AGGREGATE KEY(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10)"
types_kv_duplicate_key = "DUPLICATE KEY(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10)"
types_kv_unique_key = "UNIQUE KEY(k1, k2, k3, k4, k5, k6, k7, k8, k9, k10)"

storage_type = "column"

sandbox_column_list =  [('k0', 'TINYINT'),
                        ('k1', 'SMALLINT'),
                        ('k2', 'INT'),
                        ('k3', 'BIGINT'),
                        ('k4', 'LARGEINT'),
                        ('k5', 'decimal(9, 3)'),
                        ('k6', 'char(19)'),
                        ('k7', 'varchar(50)'),
                        ('k8', 'date'),
                        ('k9', 'datetime'),
                        ('v11', 'double', 'max'),
                        ('v12', 'float', 'sum'),
                        ('v13', 'int', 'min'),
                        ('v14', 'smallint', 'replace'),
                        ('v15', 'int', 'sum')]

sandbox_partition_key = 'k0'
sandbox_partition_name_list = ['pa', 'pb', 'pc', 'pd', 'pe', 'pf']
sandbox_partition_value_list = ['-50', '0', '32', '64', '96', 'MAXVALUE']
sandbox_partition_info = PartitionInfo(sandbox_partition_key,
                                       sandbox_partition_name_list,
                                       sandbox_partition_value_list)
sandbox_distribution_info = DistributionInfo('HASH(k2)', 5)

timestamp_convert_column_list = [('k1', 'BIGINT'),
                             ('k2', 'BIGINT'),
                             ('k3', 'BIGINT'),
                             ('k4', 'BIGINT')]

timestamp2timestamp_column_list = [('k1', 'DATETIME'),
                                   ('k2', 'BIGINT', 'MAX'),
                                   ('k3', 'BIGINT', 'MIN'),
                                   ('k4', 'BIGINT', 'REPLACE')]

date_format_column_list = [('k1', 'DATETIME'),
                           ('k2', 'DATETIME'),
                           ('k3', 'DATETIME'),
                           ('k4', 'DATETIME')]

md5_colmn_list = [('k1', 'BIGINT'),
                  ('k2', 'BIGINT'),
                  ('k3', 'BIGINT'),
                  ('k4', 'BIGINT'),
                  ('k5', 'CHAR(64)')]

replace_if_not_null_column_list = [('k1', 'BIGINT'),
                                   ('k2', 'BIGINT'),
                                   ('v1', 'TINYINT', 'SUM'),
                                   ('v2', 'TINYINT', 'REPLACE'),
                                   ('v3', 'TINYINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v4', 'SMALLINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v5', 'INT', 'REPLACE_IF_NOT_NULL'),
                                   ('v6', 'BIGINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v7', 'LARGEINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v8', 'DATETIME', 'REPLACE_IF_NOT_NULL'),
                                   ('v9', 'DATE', 'REPLACE_IF_NOT_NULL'),
                                   ('v10', 'CHAR(10)', 'REPLACE_IF_NOT_NULL'),
                                   ('v11', 'VARCHAR(6)', 'REPLACE_IF_NOT_NULL'),
                                   ('v12', 'DECIMAL(27,9)', 'REPLACE_IF_NOT_NULL')]

replace_if_not_null_no_agg_column_list = [('k1', 'BIGINT'),
                                   ('k2', 'BIGINT'),
                                   ('v1', 'TINYINT'),
                                   ('v2', 'TINYINT', 'REPLACE'),
                                   ('v3', 'TINYINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v4', 'SMALLINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v5', 'INT', 'REPLACE_IF_NOT_NULL'),
                                   ('v6', 'BIGINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v7', 'LARGEINT', 'REPLACE_IF_NOT_NULL'),
                                   ('v8', 'DATETIME', 'REPLACE_IF_NOT_NULL'),
                                   ('v9', 'DATE', 'REPLACE_IF_NOT_NULL'),
                                   ('v10', 'CHAR(10)', 'REPLACE_IF_NOT_NULL'),
                                   ('v11', 'VARCHAR(6)', 'REPLACE_IF_NOT_NULL'),
                                   ('v12', 'DECIMAL(27,9)', 'REPLACE_IF_NOT_NULL')]

boolean_column_list = [('k1', 'BOOLEAN'),
                       ('k2', 'INT'),
                       ('v1', 'BOOLEAN', 'REPLACE'),
                       ('v2', 'BOOLEAN', 'REPLACE_IF_NOT_NULL')]

boolean_column_no_agg_list = [('k1', 'BOOLEAN'),
                              ('k2', 'INT'),
                              ('v1', 'BOOLEAN')]

boolean_aggregate_key = "AGGREGATE KEY(k1, k2)"
boolean_duplicate_key = "DUPLICATE KEY(k1, k2)"
boolean_unique_key = "UNIQUE KEY(k1, k2)"
boolean_distribution_info = DistributionInfo('HASH(k1, k2)', 10)

datatype_column_list = [("k0", "BOOLEAN"),
                        ("k1", "TINYINT"),
                        ("k2", "SMALLINT"),
                        ("k3", "INT"),
                        ("k4", "BIGINT"),
                        ("k5", "LARGEINT"),
                        ("k6", "DECIMALV3(9,3)"),
                        ("k7", "CHAR(5)"),
                        ("k8", "DATE"),
                        ("k9", "DATETIME"),
                        ("k10", "VARCHAR(20)"),
                        ("k11", "DOUBLE", "max"),
                        ("k12", "FLOAT", "sum"),
                        ("k13", "HLL", "hll_union"),
                        ("k14", "BITMAP", "bitmap_union")]

datatype_column_no_agg_list  = [("k0", "BOOLEAN"),
                                ("k1", "TINYINT"),
                                ("k2", "SMALLINT"),
                                ("k3", "INT"),
                                ("k4", "BIGINT"),
                                ("k5", "LARGEINT"),
                                ("k6", "DECIMALV3(9, 3)"),
                                ("k7", "CHAR(5)"),
                                ("k8", "DATE"),
                                ("k9", "DATETIME"),
                                ("k10", "VARCHAR(20)"),
                                ("k11", "DOUBLE"),
                                ("k12", "FLOAT")]

datatype_column_uniq_key = "UNIQUE KEY(k0, k1, k2, k3, k4, k5, k6)"

string_basic = [("id", "INT"), ("doc", "STRING", "REPLACE")]

baseall_string_column_list = [("k1", "tinyint"),
                       ("k2", "smallint"),
                       ("k3", "int"),
                       ("k4", "bigint"),
                       ("k5", "decimalv3(9, 3)"),
                       ("k6", "char(5)"),
                       ("k10", "date"),
                       ("k11", "datetime"),
                       ("k7", "string", "replace"),
                       ("k8", "double", "max"),
                       ("k9", "float", "sum")]

baseall_string_no_agg_column_list = [
                       ("k1", "tinyint"),
                       ("k2", "smallint"),
                       ("k3", "int"),
                       ("k4", "bigint"),
                       ("k5", "decimal(9, 3)"),
                       ("k6", "char(5)"),
                       ("k10", "date"),
                       ("k11", "datetime"),
                       ("k7", "string"),
                       ("k8", "double"),
                       ("k9", "float")]


stable_column_list = [("k0", "int"),
                      ("k1", "tinyint"),
                       ("k2", "smallint"),
                       ("k3", "int"),
                       ("k4", "bigint"),
                       ("k5", "decimal(9, 3)"),
                       ("k6", "char(5)"),
                       ("k10", "date"),
                       ("k11", "datetime"),
                       ("k7", "varchar(20)"),
                       ("k8", "double", "max"),
                       ("k9", "float", "sum")]

# key_desc = duplicate_key
array_boolean_list = [("k1", "int"), ("k2", "array<boolean>")]
array_tinyint_list = [("k1", "int"), ("k2", "array<tinyint>")]
array_smallint_list = [("k1", "int"), ("k2", "array<smallint>")]
array_int_list = [("k1", "int"), ("k2", "array<int>")]
array_bigint_list = [("k1", "int"), ("k2", "array<bigint>")]
array_largeint_list = [("k1", "int"), ("k2", "array<largeint>")]
array_decimal_list = [("k1", "int"), ("k2", "array<decimal(27, 9)>")]
array_float_list = [("k1", "int"), ("k2", "array<float>")]
array_double_list = [("k1", "int"), ("k2", "array<double>")]
array_date_list = [("k1", "int"), ("k2", "array<date>")]
array_datetime_list = [("k1", "int"), ("k2", "array<datetime>")]
array_char_list = [("k1", "int"), ("k2", "array<char(50)>")]
array_varchar_list = [("k1", "int"), ("k2", "array<varchar(300)>")]
array_string_list = [("k1", "int"), ("k2", "array<string>")]
array_table_list = [("k1", "int"),
                    ("a1", "array<boolean>"),
                    ("a2", "array<tinyint>"),
                    ("a3", "array<smallint>"),
                    ("a4", "array<int>"),
                    ("a5", "array<bigint>"),
                    ("a6", "array<largeint>"),
                    ("a7", "array<decimal(27, 9)>"),
                    ("a8", "array<float>"),
                    ("a9", "array<double>"),
                    ("a10", "array<date>"),
                    ("a11", "array<datetime>"),
                    ("a12", "array<char(20)>"),
                    ("a13", "array<varchar(50)>"),
                    ("a14", "array<string>")]

