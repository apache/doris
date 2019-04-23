# palo_insert_utils.py使用说明

调用insert_func(host, port, user, password, database, select_sql, insert_sql)，可以做到：

1. 实现insert数据

```mysql
-- TABLE_X可以和TABLE_Y不同，[]内的内容是可选的
INSERT INTO TABLE_Y[(column1, column2,...,columnN)]
SELECT column1, column2,..., columnN [FROM TABLE_X WHERE xxx]
```

2. 判断insert是否成功。

   > demo中的实现是导入失败时，异常退出，并记录ERROR。可以自行扩展。


# check_insert_load.sh使用说明

```shell
# 调用示例
palo=``"mysql -h$palo_host -P$palo_port -u$palo_user -p$palo_password -D$palo_database"
check_insert_load_palo_func ``"$palo"` `"$sql"
```

## 附言

参考`help show load`实现的check_load_status