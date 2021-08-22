package org.apache.doris.scala

/**
 * This class is a scala demo for doris spark connector,
 * and provides four ways to read doris tables using spark.
 * before you run this class, you need to build doris-spark,
 * and put the doris-spark jar file in your maven repository
 *
 * The pom.xml dependency:
 *
 * <dependency>
 *    <groupId>org.apache</groupId>
 *    <artifactId>doris-spark</artifactId>
 *    <version>1.0.0-SNAPSHOT</version>
 *</dependency>
 * <dependency>
 *     <groupId>org.apache.spark</groupId>
 *     <artifactId>spark-core_2.11</artifactId>
 *     <version>2.3.4</version>
 * </dependency>
 * <dependency>
 *     <groupId>org.apache.spark</groupId>
 *     <artifactId>spark-sql_2.11</artifactId>
 *     <version>2.3.4</version>
 * </dependency>
 * <dependency>
 *     <groupId>org.scala-lang</groupId>
 *     <artifactId>scala-library</artifactId>
 *     <version>2.11.12</version>
 * </dependency>
 * <dependency>
 *     <groupId>mysql</groupId>
 *     <artifactId>mysql-connector-java</artifactId>
 *     <version>8.0.23</version>
 * </dependency>
 *
 * How to use:
 *
 * 1. create a table in doris with any mysql client
 *
 * CREATE TABLE `example_table` (
 * `id` bigint(20) NOT NULL COMMENT "ID",
 * `name` varchar(100) NOT NULL COMMENT "Name",
 * `age` int(11) NOT NULL COMMENT "Age"
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
 * insert into example_table values(1,"小明",21);
 * insert into example_table values(2,"小画",22);
 *
 * 3. change the Doris DORIS_DB, DORIS_TABLE, DORIS_FE_IP, DORIS_FE_HTTP_PORT,
 * DORIS_FE_QUERY_PORT, DORIS_USER, DORIS_PASSWORD config in this class
 *
 * 4. run this class, you should see the output：
 * +---+----+---+
 * | id|name|age|
 * +---+----+---+
 * |  2|  小画| 22|
 * |  1|  小明| 21|
 * +---+----+---+
 */

object DorisSparkConnectionExampleScala {

    val DORIS_DB = "demo"

    val DORIS_TABLE = "example_table"

    val DORIS_FE_IP = "127.0.0.1"

    val DORIS_FE_HTTP_PORT = "8030"

    val DORIS_FE_QUERY_PORT = "9030"

    val DORIS_USER = "root"

    val DORIS_PASSWORD = ""

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
        // if you want to run readWithRdd(sparkConf), please comment this line
        // val sc = SparkSession.builder().config(sparkConf).getOrCreate()
        readWithRdd(sparkConf)
        // readWithDataFrame(sc)
        // readWithSql(sc)
        // readWithJdbc(sc)
    }

    /**
     * read doris table Using Spark Rdd
     */
    def readWithRdd(sparkConf: SparkConf): Unit = {
        val scf = new SparkContextFunctions(new SparkContext(sparkConf))
        val rdd = scf.dorisRDD(
            tableIdentifier = Some(s"$DORIS_DB.$DORIS_TABLE"),
            cfg = Some(Map(
                "doris.fenodes" -> s"$DORIS_FE_IP:$DORIS_FE_HTTP_PORT",
                "doris.request.auth.user" -> DORIS_USER,
                "doris.request.auth.password" -> DORIS_PASSWORD
            ))
        )
        val resultArr = rdd.collect()
        println(resultArr.mkString)
    }

    /**
     * read doris table Using DataFrame
     * @param sc SparkSession
     */
    def readWithDataFrame(sc: SparkSession): Unit = {
        val df = sc.read.format("doris")
            .option("doris.table.identifier", s"$DORIS_DB.$DORIS_TABLE")
            .option("doris.fenodes", s"$DORIS_FE_IP:$DORIS_FE_HTTP_PORT")
            .option("user", DORIS_USER)
            .option("password", DORIS_PASSWORD)
            .load()
        df.show(5)
    }

    /**
     * read doris table Using Spark Sql
     * @param sc SparkSession
     */
    def readWithSql(sc: SparkSession): Unit = {
        sc.sql("CREATE TEMPORARY VIEW spark_doris\n" +
            "USING doris " +
            "OPTIONS( " +
            "  \"table.identifier\"=\"" + DORIS_DB + "." + DORIS_TABLE + "\", " +
            "  \"fenodes\"=\"" + DORIS_FE_IP + ":" + DORIS_FE_HTTP_PORT + "\", " +
            "  \"user\"=\"" + DORIS_USER + "\", " +
            "  \"password\"=\"" + DORIS_PASSWORD + "\" " +
            ")")
        sc.sql("select * from spark_doris").show(5)
    }

    /**
     * read doris table Using jdbc
     * @param sc SparkSession
     */
    def readWithJdbc(sc: SparkSession): Unit = {
        val df = sc.read.format("jdbc")
            .option("url", s"jdbc:mysql://$DORIS_FE_IP:$DORIS_FE_QUERY_PORT/$DORIS_DB?useUnicode=true&characterEncoding=utf-8")
            .option("dbtable", DORIS_TABLE)
            .option("user", DORIS_USER)
            .option("password", DORIS_PASSWORD)
            .load()
        df.show(5)
    }
}
