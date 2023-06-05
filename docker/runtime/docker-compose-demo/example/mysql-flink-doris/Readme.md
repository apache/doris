# MySQL-Flink-Doris

1. `docker-compose.yaml` 脚本中需要自行下载 `Flink-Doris-Connector`、`Flink-MySQL-Connector` 的 JAR 包，然后按目录进行映射。
2. 当前版本里可以采网络桥接模式来配置网络，也可以使用 host 模式来进行网络配置，需要进行参数调整。
3. 启动以后按照 MySQL-init、Doris-init、Flink-init 的顺序进行执行即可。
