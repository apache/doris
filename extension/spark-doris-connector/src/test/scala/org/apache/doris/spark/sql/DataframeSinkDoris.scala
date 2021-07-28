package org.apache.doris.spark.sql

import org.apache.spark.sql.SparkSession

object DataframeSinkDoris {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()

    import spark.implicits._

    val mockDataDF = List(
      (3, "440403001005", "21.cn"),
      (1, "4404030013005", "22.cn"),
      (33, null, "23.cn")
    ).toDF("id", "mi_code", "mi_name")
    mockDataDF.show(5)

    mockDataDF.write.format("doris")
      .option("beHostPort", "10.93.11.51:8043")
      .option("dbName", "example_db")
      .option("tbName", "test_insert_into")
      .option("maxRowCount", "1000")
      .option("user", "doris")
      .option("password", "Doris123!")
      .save()


  }

}
