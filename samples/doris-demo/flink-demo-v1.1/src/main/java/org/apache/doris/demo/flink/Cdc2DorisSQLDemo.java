package org.apache.doris.demo.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Cdc2DorisSQLDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        // register a table in the catalog
        tEnv.executeSql(
            "CREATE TABLE cdc_test_source (\n" +
                "  id INT,\n" +
                "  name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '127.0.0.1',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'database-name' = 'db',\n" +
                "  'table-name' = 'test_source'\n" +
                ")");
        //doris table
        tEnv.executeSql(
            "CREATE TABLE doris_test_sink (" +
                "id INT," +
                "name STRING" +
                ") " +
                "WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '127.0.0.1:8030',\n" +
                "  'table.identifier' = 'db.test_sink',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                /* doris stream load label, In the exactly-once scenario,
                   the label is globally unique and must be restarted from the latest checkpoint when restarting.
                   Exactly-once semantics can be turned off via sink.enable-2pc. */
                "  'sink.label-prefix' = 'doris_label',\n" +
                "  'sink.properties.format' = 'json',\n" +       //json data format
                "  'sink.properties.read_json_by_line' = 'true'\n" +
                ")");

        //insert into mysql table to doris table
        tEnv.executeSql("INSERT INTO doris_test_sink select id,name from cdc_test_source");
        env.execute();
    }
}
