# Broker Load

## properties

### strict mode
broker load导入可以开启strict mode模式。开启方式为 ```properties ("strict_mode" = "true")``` 。默认的 strict mode为开启。

*注意：strict mode 功能仅在新版本的broker load中有效，如果导入明确指定 ```"version" = "v1"``` 则没有此功能。*

strict mode模式的意思是：对于导入过程中的列类型转换进行严格过滤。严格过滤的策略如下：

1. 对于列类型转换来说，如果 strict\_mode 为true，则错误的数据将被filter。这里的错误数据是指：原始数据并不为空值，在参与列类型转换后结果为空值的这一类数据。

2. 对于导入的某列包含函数变换的，导入的值和函数的结果一致，strict 对其不产生影响。（其中 strftime 等 broker 系统支持的函数也属于这类）。

3. 对于导入的某列类型包含范围限制的，如果原始数据能正常通过类型转换，但无法通过范围限制的，strict 对其也不产生影响。
	+ 例如：如果类型是 decimal(1,0), 原始数据为10，则属于可以通过类型转换但不在列声明的范围内。这种数据 strict 对其不产生影响。

### strict mode 与 source data 的导入关系

这里以列类型为 TinyInt 来举例

注：当表中的列允许导入空值时

source data | source data example | string to int   | strict_mode        | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa or 2000         | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1                   | 1               | true or false      | correct data

这里以列类型为 Decimal(1,0) 举例
 
注：当表中的列允许导入空值时

source data | source data example | string to int   | strict_mode        | load_data
------------|---------------------|-----------------|--------------------|---------
空值        | \N                  | N/A             | true or false      | NULL
not null    | aaa                 | NULL            | true               | filtered
not null    | aaa                 | NULL            | false              | NULL
not null    | 1 or 10             | 1               | true or false      | correct data

*注意：10 虽然是一个超过范围的值，但是因为其类型符合decimal的要求，所以strict mode对其不产生影响。10 最后会在其他 ETL 处理流程中被过滤。但不会被strict mode过滤。*
