package org.apache.doris.spark.sql

import org.apache.spark.{SparkConf, SparkContext}



object SparkDorisConnector {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkDorisConnector").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("DEBUG")
        import org.apache.doris.spark._
        val dorisSparkRDD = sc.dorisRDD(
//            tableIdentifier = Some("ods.ods_pos_pro_shop_delta"),
            tableIdentifier = Some("ods.ods_t_pro_dish_list_detail_test_demo"),
//            query = Some("select * from ods.ods_t_pro_dish_list_detail_test_demo limit 1"),
            cfg = Some(Map(
//                "doris.fenodes" -> "10.220.146.10:8030",
                "doris.fenodes" -> "10.220.147.155:8030",
                "doris.request.auth.user" -> "root",
                "doris.request.auth.password" -> ""
            ))
        )

        dorisSparkRDD.map(println(_)).count()
        sc.stop()
    }

}
