// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

export const BASIC_COLUMN = [
    {
        title: '列名',
        dataIndex: 'field',
        key: 'field',
    },
    {
        title: '列数据类型',
        dataIndex: 'type',
        key: 'type',
    },
    {
        title: '列注释',
        dataIndex: 'comment',
        key: 'comment',
        width: 80,
    },
    {
        title: '允许空值',
        dataIndex: 'isNull',
        key: 'isNull',
        width: 80,
    },
    {
        title: '默认值',
        dataIndex: 'defaultVal',
        key: 'defaultVal',
        width: 80,
    },
];

export const TABLE_COLUMN_DUPLICATE = [
    ...BASIC_COLUMN,
    {
        title: '排序列（duplicate key）',
        dataIndex: 'key',
        key: 'key',
        width: 200,
    },
];

export const TABLE_COLUMN_AGGREGATE = [
    ...BASIC_COLUMN,
    {
        title: '维度列（aggragate key）',
        dataIndex: 'key',
        key: 'key',
    },
    {
        title: '聚合类型',
        dataIndex: 'aggrType',
        key: 'aggrType',
    },
];

export const TABLE_COLUMN_UNIQUE = [
    ...BASIC_COLUMN,
    {
        title: '主键（unique key）',
        dataIndex: 'key',
        key: 'key',
    },
];
