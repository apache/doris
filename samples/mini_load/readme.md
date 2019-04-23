# palo_mini_load_client.py使用说明

## 第一步，mini_load配置参数说明

`mini_load_host`：mini_load服务地址

`db_load_port`：mini_load服务端口号

`db_name`: 数据库用户名

`db_password`: 数据库密码

`file_name`: 待load的本地文件path

`table`: 数据库表名

## 第二步，通过PaloMiniLoadClient上传文件

```python
palo_client = PaloClient(mini_load_host,db_load_port,db_name,
												db_user,db_password, filename, table, 												load_timeout)

load_label = palo_client.load_palo()
# palo_db可以执行查询palo数据库
palo_client.check_load_status(load_label, palo_db)

```

## 附言

1. 参考`help mini load`实现的load_palo()
2. 参考`help show load`实现的check_load_status
3. palo_mini_load_client.py仅提供mini_load相关的功能代码，其中涉及到的其他工具类，可以自行实现。

