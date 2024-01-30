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
"""palo client verify"""
import petl
import math
from decimal import Decimal
from collections import OrderedDict
from datetime import datetime

import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage


class VerifyFile(object):
    """
    VerifyFile
    """

    def __init__(self, file_name, delimiter='\t'):
        self.file_name = file_name
        self.delimiter = delimiter

    def get_file_name(self):
        """
        get file name
        """
        return self.file_name

    def get_delimiter(self):
        """
        get delimiter
        """
        return self.delimiter

    def __str__(self):
        return str(self.file_name)


class Verify(object):
    """verify class"""
    def __init__(self, expected_file_list, datas, schema, table_name, database_name, encoding=None):
        """
        file：校验文件，可以是str，['file1', 'file2'] or VerifyFile
        sql_ret：sql执行的结果
        schema：verify校验使用的表的desc结果做schema
                verify_by_sql使用的4元组，sql查询结果的schema, 由四元组(name, type, agg_type, default_value)组成的list
        table_name, database_name: 生成默认的校验文件名称用
        """
        self.expected_file_list = expected_file_list
        self.table_name = table_name
        self.database_name = database_name
        self.schema = schema
        self.datas = datas
        self.encoding = encoding

    @staticmethod
    def __get_type_convert_handler(field_type):
        """"""
        def __int_type(min, max):
            """Return a function that will attempt to parse the value as a number,
            """

            def f(v):
                """check and return type
                """
                try:
                    value = int(v)
                except (ValueError, TypeError) as e:
                    raise e
                if min <= value <= max:
                    return int(v)
                else:
                    return None

            return f

        def __char_type():
            """regurn a function"""

            def f(v):
                """check v if null"""
                if v == "None":
                    v = None
                return v

            return f

        tinyint = __int_type(-2 ** 7, 2 ** 7 - 1)
        smallint = __int_type(-2 ** 15, 2 ** 15 - 1)
        paloint = __int_type(-2 ** 31, 2 ** 31 - 1)
        bigint = __int_type(-2 ** 63, 2 ** 63 - 1)
        largeint = __int_type(-2 ** 127, 2 ** 127 - 1)
        datetime = petl.datetimeparser('%Y-%m-%d %H:%M:%S')
        date = petl.dateparser('%Y-%m-%d')
        char = __char_type()
        varchar = __char_type()

        field_type = field_type.lower().split('<')[0]
        field_type = field_type.lower().split('(')[0]
        field_type_handler_dict = {'char': char, 'varchar': varchar, 'decimal': Decimal,
                                   'tinyint': tinyint, 'smallint': smallint, 'int': paloint,
                                   'bigint': bigint, 'largeint': largeint, 'text': varchar,
                                   'float': float, 'double': float, 'datetime': datetime, 'date': date,
                                   'boolean': tinyint,
                                   'array': varchar, 'decimalv3': Decimal}
        return field_type_handler_dict[field_type]

    def __get_convert_dict(self):
        """get column type from schema, and get convert func"""
        convert_dict = {field[0]: self.__get_type_convert_handler(field[1]) for field in self.schema}
        return convert_dict

    def __get_field_list(self):
        """get column name from schema"""
        field_list = [field[0] for field in self.schema]
        return field_list

    def __get_key_list(self):
        """get key column from schema"""
        key_list = [field[0] for field in self.schema if field[3] == 'true']
        return tuple(key_list)

    def __get_type_list(self):
        """get column type from schema"""
        type_list = [field[1] for field in self.schema]
        return type_list

    @staticmethod
    def __get_aggregate_key(key_list):
        """get key"""
        if len(key_list) == 1:
            return key_list[0]
        else:
            return key_list

    def __get_aggregation_ordereddict(self):
        """aggregation table value agg func"""
        def _sum(l):
            items = []
            for i in l:
                if i is not None:
                    items.append(i)
            if len(items) == 0:
                return None
            else:
                return sum(items)

        def __agg_replace(l):
            items = []
            for i in l:
                items.append(i)
            return items[-1]

        def __agg_replace_if_not_null(l):
            """ replace if not null """
            items = []
            for i in l:
                if i is not None:
                    items.append(i)
            if len(items) == 0:
                return None
            else:
                return items[-1]

        agg_function_dict = {'max': max, 'min': min, 'sum': _sum, 'replace': __agg_replace,
                             'replace_if_not_null': __agg_replace_if_not_null}

        aggregation = OrderedDict()

        aggtype_list = [(field[0], field[5]) for field in self.schema if field[5] != '']

        for item in aggtype_list:
            aggregation[item[0]] = item[0], agg_function_dict[item[1].lower()]

        return aggregation

    def __write_data_to_file(self, data_from_database, data_from_file, save_verifyfile_list):
        """将文件中的数据写入tmp文件中"""
        if self.encoding is not None:
            if save_verifyfile_list[0] is not None:
                petl.tocsv(data_from_database, save_verifyfile_list[0].get_file_name(),
                           encoding=self.encoding, delimiter=save_verifyfile_list[0].get_delimiter())
            if save_verifyfile_list[1] is not None:
                petl.tocsv(data_from_file, save_verifyfile_list[1].get_file_name(),
                           encoding=self.encoding, delimiter=save_verifyfile_list[1].get_delimiter())
        else:
            if save_verifyfile_list[0] is not None:
                petl.tocsv(data_from_database, save_verifyfile_list[0].get_file_name(),
                           delimiter=save_verifyfile_list[0].get_delimiter())
            if save_verifyfile_list[1] is not None:
                petl.tocsv(data_from_file, save_verifyfile_list[1].get_file_name(),
                           delimiter=save_verifyfile_list[1].get_delimiter())

    @staticmethod
    def __check_float(field_of_database, field_of_file, type):
        def __adjust_data(num):
            if num is None:
                 return None
            else:
                num = float(num)
                if num == 0.0:
                    return 0.0
                else:
                    return num / 10 ** (math.floor(math.log10(abs(num))) + 1)
        data_of_database = __adjust_data(field_of_database)
        data_of_file = __adjust_data(field_of_file)
        # 最后一个有效数字可以相差 1，比如： 0.123456001 == 0.123456999 => True
        # 0.123456001 == 0.123457999 => True 0.123456001 == 0.123458999 => False
        # 0.123456001 == 0.123455999 => True 0.123456001 == 0.123454999 => False
        precision = None
        if type.lower() == 'float':
            precision = 2e-6
        elif type.lower() == 'double':
            precision = 2e-15
        if math.fabs(data_of_database - data_of_file) < precision or \
                math.fabs(data_of_database - data_of_file) / data_of_file < 2e-3:
            return True
        else:
            return False

    def __check_data(self, data_from_database, data_from_file):
        rows_number_of_database = petl.nrows(data_from_database)
        rows_number_of_file = petl.nrows(data_from_file)

        if rows_number_of_database != rows_number_of_file:
            LOG.warning(L("verify data error", lines_of_database=rows_number_of_database,
                          lines_of_file=rows_number_of_file))
            return False
        result_of_database = petl.records(data_from_database)
        result_of_file = petl.records(data_from_file)
        type_list = self.__get_type_list()

        for record_of_database, record_of_file in zip(result_of_database, result_of_file):
            for field_of_database, field_of_file, field_type in \
                    zip(record_of_database, record_of_file, type_list):
                if field_of_database is None and field_of_file is None:
                    continue
                else:
                    if field_of_database is None or field_of_file is None:
                        return False
                if field_type.lower() == 'float' or field_type.lower() == 'double':
                    if not self.__check_float(field_of_database, field_of_file,
                                              type=field_type.lower()):
                        LOG.error(L("FLOAT VERIFY FAIL", field_of_database=field_of_database,
                                    field_of_file=field_of_file, record_of_database=record_of_database,
                                    record_of_file=record_of_file))
                        return False
                elif field_of_database != field_of_file:
                    LOG.error(L("VERIFY FAIL", field_of_database=field_of_database,
                                field_of_file=field_of_file, record_of_database=record_of_database,
                                record_of_file=record_of_file))
                    return False
        return True

    def __get_data_from_database(self):
        """
        处理数据库中的数据，datas是client.execute(sql)的结果
        """
        key_list = self.__get_key_list()
        header = self.__get_field_list()
        field_list = self.__get_field_list()
        convert_dict = {}
        for field in self.schema:
            if field[1].lower().startswith('largeint'):
                convert_dict[field[0]] = self.__get_type_convert_handler(field[1])
        dict_list = []
        for row in self.datas:
            field_value_dict = {}
            for field, value in zip(header, row):
                field_value_dict[field] = value
            dict_list.append(field_value_dict)
        table_database_from = petl.fromdicts(dict_list, header)
        table_database_convert = petl.convert(table_database_from, convert_dict)
        table_database_sort = petl.sort(table_database_convert, field_list)
        table_database_merge_sort = petl.mergesort(table_database_sort,
                                                   key=field_list, presorted=False)
        return table_database_merge_sort

    def __get_data_from_file(self):
        """
        从文件中获取数据，排序，按照表的聚合模型处理数据
        """
        # 为了兼容以前的代码
        if type(self.expected_file_list) is str:
            from_verifyfile_list = [VerifyFile(self.expected_file_list, '\t')]
        elif type(self.expected_file_list) is list and type(self.expected_file_list[0]) is str:
            from_verifyfile_list = [VerifyFile(file, '\t') for file in self.expected_file_list]
        elif type(self.expected_file_list) is VerifyFile:
            from_verifyfile_list = [self.expected_file_list]
        else:
            from_verifyfile_list = None
        header = self.__get_field_list()
        key_list = self.__get_key_list()
        field_list = self.__get_field_list()
        convert_dict = self.__get_convert_dict()
        dup = False
        for col in self.schema:
            if col[5] == 'NONE':
                dup = True

        table_file_to_merge_list = []
        for etl_file in from_verifyfile_list:
            # 读取csv文件数据
            table_file_from = petl.fromcsv(etl_file.get_file_name(),
                                           encoding='utf8', delimiter=etl_file.get_delimiter())
            # 给数据增加表头
            table_file_push = petl.pushheader(table_file_from, header)
            # 给数据加类型
            table_file_convert = petl.convert(table_file_push, convert_dict)
            table_file_to_merge_list.append(table_file_convert)
        if not dup:
            table_file_merge_sort = petl.mergesort(*table_file_to_merge_list,
                                                   key=key_list, presorted=False)
            aggregation = self.__get_aggregation_ordereddict()
            aggregate_key = self.__get_aggregate_key(key_list)
            # 聚合表，按照聚合方式聚合
            table_file_aggregate = petl.aggregate(table_file_merge_sort,
                                                  key=aggregate_key, aggregation=aggregation,
                                                  presorted=True)
            table_file_merge_sort = petl.mergesort(table_file_aggregate,
                                                   key=key_list, presorted=True)
            return table_file_merge_sort
        else:
            table_file_merge_sort = petl.mergesort(*table_file_to_merge_list,
                                                   key=field_list, presorted=False)
            return table_file_merge_sort

    def __generate_dafault_save_verifyfile_list(self):
        """根据库名，表名生成校验文件的名称"""
        name_prefix = ".%s.%s" % (self.database_name, self.table_name)
        name_for_database = "%s.%s" % (name_prefix, 'DB')
        name_for_file = "%s.%s" % (name_prefix, 'FILE')
        return [VerifyFile(name_for_database), VerifyFile(name_for_file)]

    def verify(self, save_file_list=None):
        """
        崭新的校验函数
        """
        LOG.info(L("check file:", file=self.expected_file_list))
        self.__adjust_schema_for_verify()
        # 获取db中的数据
        data_from_database = self.__get_data_from_database()
        # 获取file中的文件
        data_from_file = self.__get_data_from_file()
        if save_file_list is None:
            save_file_list = self.__generate_dafault_save_verifyfile_list()
        # 分别写入数据
        self.__write_data_to_file(data_from_database, data_from_file, save_file_list)
        # 返回check结果, true / false
        return self.__check_data(data_from_database, data_from_file)

    def __adjust_schema_for_verify(self):
        adjust_schema = []
        for field in self.schema:
            adjust_field = list(field)
            if adjust_field[3] == 'false':
                if adjust_field[5] is not None:
                    adjust_field[5] = adjust_field[5].split(',')[0]
            else:
                adjust_field[5] = ''
            adjust_schema.append(tuple(adjust_field))
        self.schema = tuple(adjust_schema)
        return self.schema

    def __adjust_schema_for_self_defined_sql(self):
        # TODO
        # 这个函数可能有问题，以后修改
        adjust_schema = []
        for column in self.schema:
            adjust_column = []
            adjust_column.append(column[0])
            adjust_column.append(column[1])
            adjust_column.append('No')
            if len(column) > 2 and column[2] is not None:
                adjust_column.append('false')
            else:
                adjust_column.append('true')
            adjust_column.append('N/A')
            if len(column) > 2 and column[2] is not None:
                adjust_column.append(column[2])
            else:
                adjust_column.append('')
            adjust_schema.append(tuple(adjust_column))
        self.schema = adjust_schema
        return self.schema

    def verify_by_sql(self, save_file_list=None):
        """
        校验自定义的SQL语句的查询结果
        expected_file_list: VerifyFile对象的list
        sql: SQL语句字符串
        schema: 查询结果的schema, 由四元组(name, type, agg_type, default_value)组成的list
        四元组中后两项可省略, 需要注意的是key列指定默认值是agg_type设置为None
        Example -> [("k1", "INT"), ("k2", "CHAR", None, ""), ("v", "DATE", "REPLACE")]
        save_file_list: VerifyFile对象的list
        """
        self.__adjust_schema_for_self_defined_sql()
        data_from_database = self.__get_data_from_database()
        data_from_file = self.__get_data_from_file()
        if save_file_list is not None:
            self.__write_data_to_file(data_from_database, data_from_file, save_file_list)
        return self.__check_data(data_from_database, data_from_file)


def verify(file, sql_ret, schema, table_name, database_name, encoding, save_file_list):
    """
    verify, schema为palo desc结果
    适用于
    1. 多个文件的时候，会对文件进行拼接，排序读取
    2. 适用于原始文件，palo对原始文件进行过滤、聚合等处理时，无需额外保存校验文件，直接使用原始文件进行处理生成校验文件
    """
    verifier = Verify(file, sql_ret, schema, table_name, database_name, encoding)
    return verifier.verify(save_file_list)


def verify_by_sql(file, sql_ret, schema, table_name, database_name, encoding, save_file_list):
    """
    verify by sql
    指定四元组为schema
    """
    verifier = Verify(file, sql_ret, schema, table_name, database_name, encoding)
    return verifier.verify_by_sql(save_file_list)

