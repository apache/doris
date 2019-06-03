# broker load 使用说明

## properties

### strict mode
broker load导入可以开启strict mode模式。开启方式为 ```properties ("strict_mode" = "true")``` 。默认的 strict mode为开启。

*注意：strict mode 功能仅在新版本的broker load中有效，如果导入明确指定 ```"version" = "v1"``` 则没有此功能。*

strict mode模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

对于列类型转换来说，如果 strict\_mode 为true，则错误的数据将被filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

对于导入的某列包含函数变换的，导入的值和函数的结果一致，strict 对其不产生影响。（其中 strftime 等 broker 系统支持的函数也属于这类）。

### strict mode 与 source data 的导入关系

这里以列类型为 int 来举例
注：当表中的列允许导入空值时

source data | source data example | string to int   | strict_mode        | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa                 | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1                   | 1               | true or false      | correct data
