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
    },
    {
        title: '允许空值',
        dataIndex: 'isNull',
        key: 'isNull',
    },
    {
        title: '默认值',
        dataIndex: 'defaultVal',
        key: 'defaultVal',
    },
];

export const TABLE_COLUMN_DUPLICATE = [
    ...BASIC_COLUMN,
    {
        title: '排序列（duplicate key）',
        dataIndex: 'key',
        key: 'key',
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
