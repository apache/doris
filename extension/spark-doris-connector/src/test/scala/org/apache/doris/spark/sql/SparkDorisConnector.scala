package org.apache.doris.spark.sql

import org.apache.spark.{SparkConf, SparkContext}



object SparkDorisConnector {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkDorisConnector").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("DEBUG")
        import org.apache.doris.spark._
        val dorisSparkRDD = sc.dorisRDD(
            tableIdentifier = Some("db.table1"),
            cfg = Some(Map(
                "doris.fenodes" -> "feip:8030",
                "doris.request.auth.user" -> "root",
                "doris.request.auth.password" -> ""
            ))
        )

        dorisSparkRDD.map(println(_)).count()
        sc.stop()
    }

}
