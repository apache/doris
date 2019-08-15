# 时区

Doris 支持多时区设置

## 名词解释

* FE：Frontend，Doris 的前端节点。负责元数据管理和请求接入。
* BE：Backend，Doris 的后端节点。负责查询执行和数据存储。

## 基本概念

Doris 内部存在多个时区相关参数

* system_time_zone :
    当服务器启动时，会根据机器设置时区自动设置，设置后不可修改。
    
* time_zone :
    服务器当前时区，区分session级别和global级别

## 具体操作

1. show variables like '%time_zone%'

    查看当前时区相关配置
    
2. SET time_zone = 'Asia/Shanghai'

    该命令可以设置session级别的时区，连接断开后失效
    
3. SET global time_zone = 'Asia/Shanghai'

    该命令可以设置global级别的时区参数，fe会将参数持久化，连接断开后不失效
    
### 时区的影响

时区设置会影响对时区敏感的时间值的显示和存储。

包括NOW()或CURTIME()等时间函数显示的值，也包括show load, show backends中的时间值。

但不会影响create table 中时间类型分区列的less than值，也不会影响存储为date/datetime类型的值的显示。

## 使用限制

时区值可以使用几种格式给出，不区分大小写:

* 表示UTC偏移量的字符串，如'+10:00'或'-6:00'

* 标准时区格式，如"Asia/Shanghai"、"America/Los_Angeles"

* 不支持缩写时区格式，如"MET"、"CTT"。因为缩写时区在不同场景下存在歧义，不建议使用。

* 为了兼容Doris，支持CST缩写时区，内部会将CST转移为"Asia/Shanghai"的中国标准时区

