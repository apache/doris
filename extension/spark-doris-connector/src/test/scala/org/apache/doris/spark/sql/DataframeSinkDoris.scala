// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
      .option("feHostPort", "10.211.55.9:8030")
      .option("dbName", "example_db")
      .option("tbName", "test_insert_into")
      .option("maxRowCount", "1000")
      .option("user", "root")
      .option("password", "")
      .save()


  }

}
