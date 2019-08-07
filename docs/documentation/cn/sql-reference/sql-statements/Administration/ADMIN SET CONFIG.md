# ADMIN SET CONFIG
## description

    该语句用于设置集群的配置项（当前仅支持设置FE的配置项）。
    可设置的配置项，可以通过 AMDIN SHOW FRONTEND CONFIG; 命令查看。

    语法：

        ADMIN SET FRONTEND CONFIG ("key" = "value");

## example

    1. 设置 'disable_balance' 为 true

        ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");

## keyword
    ADMIN,SET,CONFIG
