package org.apache.doris;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.HashMap;
import java.util.Map;

public class TestPG {
    public static void main(String[] args) throws Exception {
        Map<String, Object> customConverterConfigs = new HashMap<>();
        DebeziumDeserializationSchema schema =
                new JsonDebeziumDeserializationSchema(false, customConverterConfigs);

        JdbcIncrementalSource<String> incrSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname("10.16.10.6")
                        .port(5438)
                        .database("test_db")
                        .slotName("x12")
                        .schemaList("public")
                        .tableList("public.t_user_info")
                        .username("postgres")
                        .password("postgres")
                        .decodingPluginName("pgoutput")
                        .deserializer(schema)
                        .startupOptions(StartupOptions.latest())
                        // .splitSize(1)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(10000);
        env.fromSource(incrSource, WatermarkStrategy.noWatermarks(), "Postgres CDC Source").print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
