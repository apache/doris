# CREATE FUNCTION

## Syntax

```
CREATE [AGGREGATE] FUNCTION function_name
    (arg_type [, ...])
    RETURNS ret_type
    [INTERMEDIATE inter_type]
    [PROPERTIES ("key" = "value" [, ...]) ]
```

## Description

此语句创建一个自定义函数。执行此命令需要用户拥有 `ADMIN` 权限。

如果 `function_name` 中包含了数据库名字，那么这个自定义函数会创建在对应的数据库中，否则这个函数将会创建在当前会话所在的数据库。新函数的名字与参数不能够与当前命名空间中已存在的函数相同，否则会创建失败。但是只有名字相同，参数不同是能够创建成功的。

## Parameters

> `AGGREGATE`: 如果有此项，表示的是创建的函数是一个聚合函数，否则创建的是一个标量函数。
>
> `function_name`: 要创建函数的名字, 可以包含数据库的名字。比如：`db1.my_func`。
>
> `arg_type`: 函数的参数类型，与建表时定义的类型一致。变长参数时可以使用`, ...`来表示，如果是变长类型，那么变长部分参数的类型与最后一个非变长参数类型一致。
> 
> `ret_type`: 函数返回类型。
> 
> `inter_type`: 用于表示聚合函数中间阶段的数据类型。
> 
> `properties`: 用于设定此函数相关属性，能够设置的属性包括
>       
>           "object_file": 自定义函数动态库的URL路径，当前只支持 HTTP/HTTPS 协议，此路径需要在函数整个生命周期内保持有效。此选项为必选项
>
>           "symbol": 标量函数的函数签名，用于从动态库里面找到函数入口。此选项对于标量函数是必选项
>
>           "init_fn": 聚合函数的初始化函数签名。对于聚合函数是必选项
>
>           "update_fn": 聚合函数的更新函数签名。对于聚合函数是必选项
>           
>           "merge_fn": 聚合函数的合并函数签名。对于聚合函数是必选项
>           
>           "serialize_fn": 聚合函数的序列化函数签名。对于聚合函数是可选项，如果没有指定，那么将会使用默认的序列化函数
>           
>           "finalize_fn": 聚合函数获取最后结果的函数签名。对于聚合函数是可选项，如果没有指定，将会使用默认的获取结果函数
>
>           "md5": 函数动态链接库的MD5值，用于校验下载的内容是否正确。此选项是可选项

## Examples

1. 创建一个自定义标量函数

```
CREATE FUNCTION my_add(INT, INT) RETURNS INT PROPERTIES (
    "symbol" = "_ZN9doris_udf6AddUdfEPNS_15FunctionContextERKNS_6IntValES4_",
    "object_file" = "http://host:port/libmyadd.so"
);
```

2. 创建一个自定义聚合函数

```
CREATE AGGREGATE FUNCTION my_count (BIGINT) RETURNS BIGINT PROPERTIES (
    "init_fn"="_ZN9doris_udf9CountInitEPNS_15FunctionContextEPNS_9BigIntValE",
    "update_fn"="_ZN9doris_udf11CountUpdateEPNS_15FunctionContextERKNS_6IntValEPNS_9BigIntValE",
    "merge_fn"="_ZN9doris_udf10CountMergeEPNS_15FunctionContextERKNS_9BigIntValEPS2_",
    "finalize_fn"="_ZN9doris_udf13CountFinalizeEPNS_15FunctionContextERKNS_9BigIntValE",
    "object_file"="http://host:port/libudasample.so"
);
```
