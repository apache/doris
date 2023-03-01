

unary_arithmetic = ['~', '!', 'div']
binary_arithmetic = ['+', '-', '*', '/', '%']
bit_arithmetic = ['BITAND', 'BITOR', 'BITXOR', '&', '|', '^', '~']

type_to_column_map = {
    'TinyInt': ['ktint'],
    'SmallInt': ['ksint'],
    'Integer': ['kint'],
    'BigInt': ['kbint'],
    'LargeInt': ['klint'],
    'Float': ['kfloat'],
    'Double': ['kdbl'],
    'DecimalV2': ['kdcml'],
    'Char': ['kchrs1'],
    'Varchar': ['kvchrs1'],
    'String': ['kstr'],
    'Date': ['kdt'],
    'DateTime': ['kdtm'],
    'DateV2': ['kdtv2'],
    'DateTimeV2': ['kdtmv2'],
    'Boolean': ['kbool'],
}

type_set = {
    'int': ['Boolean', 'TinyInt', 'SmallInt', 'Integer', 'BigInt', 'LargeInt'],
    'float': ['Float', 'Double'],
    'string': ['Char', 'Varchar', 'String'],
    'date': ['Date', 'DateTime', 'DateV2', 'DateTimeV2']
}
