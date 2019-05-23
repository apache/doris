# DROP FUNCTION

## Syntax

```
DROP FUNCTION function_name
    (arg_type [, ...])
```

## Description

删除一个自定义函数。函数的名字、参数类型完全一致才能够被删除

## Parameters

> `function_name`: 要删除函数的名字
> 
> `arg_type`: 要删除函数的参数列表
> 

## Examples

1. 删除掉一个函数

```
DROP FUNCTION my_add(INT, INT)
```
