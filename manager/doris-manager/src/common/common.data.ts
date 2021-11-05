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

export enum FieldTypeEnum {
    TINYINT = 'TINYINT',
    SMALLINT = 'SMALLINT',
    INT = 'INT',
    BIGINT = 'BIGINT',
    LARGEINT = 'LARGEINT',
    BOOLEAN = 'BOOLEAN',
    FLOAT = 'FLOAT',
    DOUBLE = 'DOUBLE',
    DECIMAL = 'DECIMAL',
    DATE = 'DATE',
    DATETIME = 'DATETIME',
    CHAR = 'CHAR',
    VARCHAR = 'VARCHAR',
    // HLL = 'HLL',
    BITMAP = 'BITMAP',
}
export enum ConfigurationTypeEnum {
    FE = 'fe',
    BE = 'be',
}
export enum TableTypeEnum {
    PRIMARY_KEYS = 'PRIMARY_KEYS',
    UNIQUE_KEYS = 'UNIQUE_KEYS',
    DUP_KEYS = 'DUP_KEYS',
    AGG_KEYS = 'AGG_KEYS',
}


export const TABLE_TYPE_KEYS = [
    {
        value: TableTypeEnum.PRIMARY_KEYS,
        text: '',
    },
    {
        value: TableTypeEnum.UNIQUE_KEYS,
        text: '主键唯一表',
    },
    {
        value: TableTypeEnum.DUP_KEYS,
        text: '明细表',
    },
    {
        value: TableTypeEnum.AGG_KEYS,
        text: '聚合表',
    },
];

export const FIELD_TYPES: string[] = [];
for (const fieldType in FieldTypeEnum) {
    if (typeof fieldType !== 'number') {
        FIELD_TYPES.push(fieldType);
    }
}

// 首列不能是以下类型
const FIRST_COLUMN_FIELD_TYPE_CANNOT_BE = [
    FieldTypeEnum.BITMAP,
    // FieldTypeEnum.HLL,
    FieldTypeEnum.FLOAT,
    FieldTypeEnum.DOUBLE,
];
// 分桶列必须是以下类型
export const BUCKET_MUST_BE = [
    FieldTypeEnum.VARCHAR,
    FieldTypeEnum.BIGINT,
    FieldTypeEnum.INT,
    FieldTypeEnum.LARGEINT,
    FieldTypeEnum.SMALLINT,
    FieldTypeEnum.TINYINT,
];

export const FIRST_COLUMN_FIELD_TYPES = FIELD_TYPES.filter(
    (field: any) => !FIRST_COLUMN_FIELD_TYPE_CANNOT_BE.includes(field),
);

export const ANALYTICS_URL = '/login';
export const STUDIO_INDEX_URL = `/meta/index`; //待确认
export const MANAGE_INDEX_URL = `/meta/index`;
