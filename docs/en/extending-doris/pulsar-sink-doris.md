## Pulsar IO Doris

Pulsar IO Doris can synchronize data stored in Pulsar to Doris.

## Version Compatibility

| Connector | Pulsar | Doris | Java |
| --------- | ------ | ----- | ---- |
| 1.0.0     | 2.8.0+ | 0.13+ | 8    |

## Build and Install

Execute following command in dir `extension/pulsar-io-doris/` ：

```bash
sh build.sh
```

After successful compilation, the file `pulsar-io-doris-2.8.0.nar`  will be generated in the `output/` directory. Create a folder `connector` under the installation directory in the `Pulsar cluster`, then copy this file to it, and then start the sink operation through the nar package location.

## Configure

Before using the Doris sink connector, you need to configure it. You can create a configuration file (JSON or YAML) to set the following properties.

| Name                       | Type   | Required | Default            | Description                                                  |
| -------------------------- | ------ | -------- | ------------------ | ------------------------------------------------------------ |
| `doris_host`               | String | true     | "xxx.com"          | A comma-separated list of hosts, which are the addresses of Doris Fe services.It is recommended that Doris Fe service be proxy. |
| `doris_db`                 | String | true     | " " (empty string) | The database that this connector connects to.                |
| `doris_table`              | String | true     | " " (empty string) | The Table connected by this connector.                       |
| `doris_user`               | String | true     | "root"             | Username used to connect to Doris.                           |
| `doris_password`           | String | true     | " " (empty string) | Password used to connect to the Doris.                       |
| `doris_http_port`          | String | true     | "8030"             | Http server port on Doris FE.                                |
| `job_failure_retries`      | String | false    | "2"                | Number of job failure retries.                               |
| `job_label_repeat_retries` | String | false    | "3"                | Because the job label is repeated, the maximum number of repeated submissions is limited. |
| `timeout`                  | int    | true     | 500                | Insert into Doris timeout in milliseconds.                   |
| `batchSize`                | int    | true     | 200                | The batch size of updates made to the Doris.                 |

## Example

1、Create a configuration file before using the Pulsar Doris connector.

You can use one of the following methods to create a configuration file:

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

2、Prepare Pulsar service

For more information, please refer to [pulsar deployment](https://pulsar.apache.org/docs/zh-CN/standalone/).

3、Copy the NAR package of the Doris connector to the Pulsar connector directory.

mkdir  ${PULSAR_HOME}/connectors/

cp  extension/pulsar-io-doris/output/pulsar-io-doris-2.8.0.nar  ${PULSAR_HOME}/connectors/

4、Start Pulsar in standalone mode.

```bash
${PULSAR_HOME}/bin/pulsar-daemon start standalone
```

5、Run the Doris receiver connector locally.

```bash
${PULSAR_HOME}/bin/pulsar-admin sink localrun --sink-config-file connectors/pulsar-io-doris-config.yaml
```

6、Create a database and table in doris to test.

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

7、Send message to Pulsar topics.

There is a `DorisPulsarIOTest.java` in the source directory of `extension/pulsar-io-doris/src/test/java/org/apache/pulsar/io/doris/`, execute the producerMessage method

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

8、Query data in doris.

![image-20210712162543451](https://i.loli.net/2021/07/12/lIbVqfL4SEpzeh6.png)

