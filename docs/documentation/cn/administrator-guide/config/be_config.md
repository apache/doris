# be 配置项说明
## enable load strict 配置说明

be 的配置中有一个参数 enable_load_strict 用于限制所有导入方式中类型转换如果遇到错误数据，是否严格 filter。

enable load strict 参数只对导入中的类型转换有效，对于类型转换来说，如果 enable_load_strict 为true，则错误的数据将被filter。

对于导入的某列包含函数变换的，导入的值和函数的结果一致，strict 对其不产生影响。（其中 strftime 等 broker 系统支持的函数也属于这类）。

### strict 与类型转换关系

这里以列类型为 int 来举例
注：当表中的列允许导入空值时

source data | source data example | string to int   | enable_load_strict | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa                 | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1                   | 1               | true or false      | correct data
