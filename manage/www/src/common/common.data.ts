import { DEFAULT_NAMESPACE_ID } from '@src/config';

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
export const METE_INDEX_URL = `/${DEFAULT_NAMESPACE_ID}/meta/index`;
