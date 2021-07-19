## Pulsar IO Doris

Pulsar IO Doris可以同步Pulsar中存储的数据到Doris。

## 版本兼容

| Connector | Pulsar | Doris | Java |
| --------- | ------ | ----- | ---- |
| 1.0.0     | 2.8.0+ | 0.13+ | 8    |

## 编译与安装

在 `extension/pulsar-io-doris/` 源码目录下执行：

```bash
sh build.sh
```

**直接编译可能会报错，你可以先把src/test/java/org/apache/pulsar/io/doris/DorisPulsarIOTest.java中的producerMessage和testSendData方法注释掉即可**

编译成功后，会在 `output/` 目录下生成文件 `pulsar-io-doris-2.8.0.nar`。 在`Pulsar集群` 中的安装目录下创建文件夹 `connector` ，然后把此文件复制到其中，之后通过nar包位置来启动sink操作。

## 配置

在使用Doris sink连接器之前，您需要对其进行配置。您可以创建配置文件（JSON 或 YAML）来设置以下属性。

| Name                       | Type   | Required | Default            | Description                                                  |
| -------------------------- | ------ | -------- | ------------------ | ------------------------------------------------------------ |
| `doris_host`               | String | true     | "xxx.com"          | 逗号分隔的hosts列表，是Doris Fe服务的地址。推荐Doris Fe服务为proxy。 |
| `doris_db`                 | String | true     | " " (empty string) | 此连接器连接到的数据库。                                     |
| `doris_table`              | String | true     | " " (empty string) | 该连接器连接的表。                                           |
| `doris_user`               | String | true     | "root"             | 用于连接到 Doris 的用户名。                                  |
| `doris_password`           | String | true     | " " (empty string) | 用于连接 Doris 的密码。                                      |
| `doris_http_port`          | String | true     | "8030"             | Doris FE 上的 Http 服务器端口。                              |
| `job_failure_retries`      | String | false    | "2"                | 作业失败重试次数。                                           |
| `job_label_repeat_retries` | String | false    | "3"                | 作业标签导致的重复提交的最大次数。                           |
| `timeout`                  | int    | true     | 500                | 以毫秒为单位插入 Doris 超时。                                |
| `batchSize`                | int    | true     | 200                | 对 Doris 进行的更新的批量消息条数。                          |

## 使用示例

1、在使用 Pulsar Doris连接器之前创建一个配置文件。

可以使用以下方法之一来创建配置文件：

- JSON

```json
{
    "tenant": "public",
    "namespace": "default",
    "name": "doris-test-sink",
    "inputs": ["doris-sink-topic"],
    "archive": "connectors/pulsar-io-doris-2.8.0.nar",
    "parallelism": 1,
    "configs":
    {
        "doris_host": "127.0.0.1",
        "doris_db": "db1",
        "doris_table": "stream_test",
        "doris_user": "root",
        "doris_password": "",
        "doris_http_port": "8030",
        "job_failure_retries": "2",
        "job_label_repeat_retries": "3",
        "timeout": 1000,
        "batchSize": 100
    }
}
```

- YAML

```yaml
tenant: "public"
namespace: "default"
name: "doris-test-sink"
inputs: 
  - "doris-sink-topic"
archive: "connectors/pulsar-io-doris-2.8.0.nar"
parallelism: 1

configs:
    doris_host: "127.0.0.1"
    doris_db: "db1"
    doris_table: "stream_test"
    doris_user: "root"
    doris_password: ""
    doris_http_port: "8030"
    job_failure_retries: "2"
    job_label_repeat_retries: "3"
    timeout: 1000
    batchSize: 100
```

2、准备Pulsar服务

有关更多信息，请参阅[pulsar部署](https://pulsar.apache.org/docs/zh-CN/standalone/)

3、将 Doris连接器的 NAR 包复制到 Pulsar 连接器目录。

mkdir  ${PULSAR_HOME}/connectors/

cp  extension/pulsar-io-doris/output/pulsar-io-doris-2.8.0.nar  ${PULSAR_HOME}/connectors/

4、以独立模式启动 Pulsar。

```bash
${PULSAR_HOME}/bin/pulsar-daemon start standalone
```

5、在本地运行Doris接收器连接器。

```bash
${PULSAR_HOME}/bin/pulsar-admin sink localrun --sink-config-file connectors/pulsar-io-doris-config.yaml
```

6、在doris中创建数据库和表来测试。

```sql
create database db1;

use db1;

CREATE TABLE `stream_test` (
  `id` bigint(20) COMMENT "",
  `id2` bigint(20) COMMENT "",
  `username` varchar(32) COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 20
PROPERTIES(
  "replication_num" = "1",
  "strict_mode" = "true"
);
```

7、发送message到Pulsar topics。

在 `extension/pulsar-io-doris/src/test/java/org/apache/pulsar/io/doris/` 源码目录下有一个`DorisPulsarIOTest.java`，执行producerMessage方法

```java
@Slf4j
public class DorisPulsarIOTest {
    private static final String TOPIC = "doris-sink";

    @Test
    public void producerMessage() {
        final String pulsarServiceUrl = "pulsar://localhost:6650";
        try (PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsarServiceUrl)
                .build()) {
            RecordSchemaBuilder schemaBuilder = SchemaBuilder.record(
                    "io.streamnative.examples.schema.json"
            );
            schemaBuilder.field("id")
                    .type(SchemaType.INT64)
                    .required();
            schemaBuilder.field("id2")
                    .type(SchemaType.INT64)
                    .required();
            schemaBuilder.field("username")
                    .type(SchemaType.STRING)
                    .required();
            SchemaInfo schemaInfo = schemaBuilder.build(SchemaType.JSON);
            GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);
            try (Producer<GenericRecord> producer = client.newProducer(schema)
                    .topic(TOPIC)
                    .create()) {
                final int numMessages = 1000;
                for (long i = 0L; i < numMessages; i++) {
                    final long id = i;
                    final long id2 = i + 1L;
                    String username = "user-" + i;
                    GenericRecord record = schema.newRecordBuilder()
                            .set("id", id)
                            .set("id2", id2)
                            .set("username", username)
                            .build();
                    // send the payment in an async way
                    producer.newMessage()
                            .key(username)
                            .value(record)
                            .sendAsync();
                    if (i % 100 == 0) {
                        Thread.sleep(200);
                    }
                }
                // flush out all outstanding messages
                producer.flush();
                System.out.printf("Successfully produced %d messages to a topic called %s%n",
                        numMessages, TOPIC);
            }
        } catch (PulsarClientException | InterruptedException e) {
            System.err.println("Failed to produce generic avro messages to pulsar:");
            e.printStackTrace();
            Runtime.getRuntime().exit(-1);
        }
    }
}
```

8、在doris中查询数据。

![image-20210712162543451](https://i.loli.net/2021/07/12/lIbVqfL4SEpzeh6.png)

