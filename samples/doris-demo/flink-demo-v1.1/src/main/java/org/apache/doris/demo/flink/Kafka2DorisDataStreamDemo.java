package org.apache.doris.demo.flink;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.DorisStreamLoad;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Kafka2DorisDataStreamDemo {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("group.id", "group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //source config
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("test-topic",new SimpleStringSchema(),props);

        //sink config
        DorisSink.Builder<String> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();
        dorisBuilder.setFenodes("127.0.0.1:8030")
            .setTableIdentifier("db.table")
            .setUsername("root")
            .setPassword("password");

        Properties pro = new Properties();
        //json data format
        pro.setProperty("format", "json");
        pro.setProperty("read_json_by_line", "true");
        DorisExecutionOptions  executionOptions = DorisExecutionOptions.builder()
                                                        .setLabelPrefix("label-doris") //streamload label prefix,
                                                        .setStreamLoadProp(pro).build();

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
            .setDorisExecutionOptions(executionOptions)
            .setSerializer(new SimpleStringSerializer()) //serialize according to string
            .setDorisOptions(dorisBuilder.build());

        //build stream
        DataStreamSource<String> dataStreamSource = env.addSource(flinkKafkaConsumer);
        dataStreamSource.sinkTo(builder.build());

        env.execute("flink kafka to doris by datastream");
    }
}
