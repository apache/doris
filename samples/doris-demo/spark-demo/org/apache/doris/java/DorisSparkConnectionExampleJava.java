package org.apache.doris.java;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class is a java demo for doris spark connector,
 * and provides three ways to read doris table using spark.
 * before you run this class, you need to build doris-spark,
 * and put the doris-spark jar file in your maven repository
 *
 * The pom.xml dependency:
 *
 *<dependency>
 *    <groupId>org.apache</groupId>
 *    <artifactId>doris-spark</artifactId>
 *    <version>1.0.0-SNAPSHOT</version>
 *</dependency>
 *<dependency>
 *    <groupId>org.apache.spark</groupId>
 *    <artifactId>spark-core_2.11</artifactId>
 *    <version>2.3.4</version>
 *</dependency>
 *<dependency>
 *    <groupId>org.apache.spark</groupId>
 *    <artifactId>spark-sql_2.11</artifactId>
 *    <version>2.3.4</version>
 *</dependency>
 *<dependency>
 *    <groupId>org.scala-lang</groupId>
 *    <artifactId>scala-library</artifactId>
 *    <version>2.11.12</version>
 *</dependency>
 *<dependency>
 *    <groupId>mysql</groupId>
 *    <artifactId>mysql-connector-java</artifactId>
 *    <version>8.0.23</version>
 *</dependency>
 *
 * How to use:
 *
 * 1. create a table in doris with any mysql client
 *
 * CREATE TABLE `example_table` (
 *   `id` bigint(20) NOT NULL COMMENT "ID",
 *   `name` varchar(100) NOT NULL COMMENT "Name",
 *   `age` int(11) NOT NULL COMMENT "Age"
 * ) ENGINE=OLAP
 * UNIQUE KEY(`id`)
 * COMMENT "example table"
 * DISTRIBUTED BY HASH(`id`) BUCKETS 1
 * PROPERTIES (
 * "replication_num" = "1",
 * "in_memory" = "false",
 * "storage_format" = "V2"
 * );
 *
 * 2. insert some data to example_table
 *    insert into example_table values(1,"小明",21);
 *    insert into example_table values(2,"小画",22);
 *
 * 3. change the Doris DORIS_DB, DORIS_TABLE, DORIS_FE_IP, DORIS_FE_HTTP_PORT,
 *    DORIS_FE_QUERY_PORT, DORIS_USER, DORIS_PASSWORD config in this class
 *
 * 4. run this class, you should see the output：
 * +---+----+---+
 * | id|name|age|
 * +---+----+---+
 * |  2|  小画| 22|
 * |  1|  小明| 21|
 * +---+----+---+
 */
public class DorisSparkConnectionExampleJava {

    private static final String DORIS_DB = "demo";

    private static final String DORIS_TABLE = "example_table";

    private static final String DORIS_FE_IP = "127.0.0.1";

    private static final String DORIS_FE_HTTP_PORT = "8030";

    private static final String DORIS_FE_QUERY_PORT = "9030";

    private static final String DORIS_USER = "root";

    private static final String DORIS_PASSWORD = "";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[*]");
        SparkSession sc = SparkSession.builder().config(sparkConf).getOrCreate();
        readWithDataFrame(sc);
        //readWithSparkSql(sc);
        //readWithJdbc(sc);
    }

    /**
     * read doris table Using DataFrame
     * @param sc SparkSession
     */
    private static void readWithDataFrame(SparkSession sc) {
        Dataset<Row> df = sc.read().format("doris")
                .option("doris.table.identifier", String.format("%s.%s", DORIS_DB, DORIS_TABLE))
                .option("doris.fenodes", String.format("%s:%s", DORIS_FE_IP, DORIS_FE_HTTP_PORT))
                .option("user", DORIS_USER)
                .option("password", DORIS_PASSWORD)
                .load();
        df.show(5);
    }

    /**
     * read doris table Using Spark Sql
     * @param sc SparkSession
     */
    private static void readWithSparkSql(SparkSession sc) {
        sc.sql("CREATE TEMPORARY VIEW spark_doris " +
                "USING doris " +
                "OPTIONS( " +
                "  \"table.identifier\"=\"" + DORIS_DB + "." + DORIS_TABLE + "\", " +
                "  \"fenodes\"=\"" + DORIS_FE_IP + ":" + DORIS_FE_HTTP_PORT + "\", " +
                "  \"user\"=\"" + DORIS_USER + "\", " +
                "  \"password\"=\"" + DORIS_PASSWORD + "\" " +
                ")");
        sc.sql("select * from spark_doris").show(5);
    }

    /**
     * read doris table Using jdbc
     * @param sc SparkSession
     */
    private static void readWithJdbc(SparkSession sc) {
        Dataset<Row> df = sc.read().format("jdbc")
                .option("url", String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8", DORIS_FE_IP, DORIS_FE_QUERY_PORT, DORIS_DB))
                .option("dbtable", DORIS_TABLE)
                .option("user", DORIS_USER)
                .option("password", DORIS_PASSWORD)
                .load();
        df.show(5);
    }
}
