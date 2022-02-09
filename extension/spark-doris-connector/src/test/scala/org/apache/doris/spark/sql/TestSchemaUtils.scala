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

import org.apache.doris.spark.exception.DorisException
import org.apache.doris.spark.rest.models.{Field, Schema}
import org.apache.doris.thrift.{TPrimitiveType, TScanColumnDesc}
import org.apache.spark.sql.types._
import org.hamcrest.core.StringStartsWith.startsWith
import org.junit.{Assert, Test}

import scala.collection.JavaConverters._

class TestSchemaUtils extends ExpectedExceptionTest {
  @Test
  def testConvertToStruct(): Unit = {
    val schema = new Schema
    schema.setStatus(200)
    val k1 = new Field("k1", "TINYINT", "", 0, 0, "")
    val k5 = new Field("k5", "BIGINT", "", 0, 0, "")
    schema.put(k1)
    schema.put(k5)

    var fields = List[StructField]()
    fields :+= DataTypes.createStructField("k1", DataTypes.ByteType, true)
    fields :+= DataTypes.createStructField("k5", DataTypes.LongType, true)
    val expected = DataTypes.createStructType(fields.asJava)
    Assert.assertEquals(expected, SchemaUtils.convertToStruct(schema))
  }

  @Test
  def testGetCatalystType(): Unit = {
    Assert.assertEquals(DataTypes.NullType, SchemaUtils.getCatalystType("NULL_TYPE", 0, 0))
    Assert.assertEquals(DataTypes.BooleanType, SchemaUtils.getCatalystType("BOOLEAN", 0, 0))
    Assert.assertEquals(DataTypes.ByteType, SchemaUtils.getCatalystType("TINYINT", 0, 0))
    Assert.assertEquals(DataTypes.ShortType, SchemaUtils.getCatalystType("SMALLINT", 0, 0))
    Assert.assertEquals(DataTypes.IntegerType, SchemaUtils.getCatalystType("INT", 0, 0))
    Assert.assertEquals(DataTypes.LongType, SchemaUtils.getCatalystType("BIGINT", 0, 0))
    Assert.assertEquals(DataTypes.FloatType, SchemaUtils.getCatalystType("FLOAT", 0, 0))
    Assert.assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("DOUBLE", 0, 0))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATE", 0, 0))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("DATETIME", 0, 0))
    Assert.assertEquals(DataTypes.BinaryType, SchemaUtils.getCatalystType("BINARY", 0, 0))
    Assert.assertEquals(DecimalType(9, 3), SchemaUtils.getCatalystType("DECIMAL", 9, 3))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("CHAR", 0, 0))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("LARGEINT", 0, 0))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("VARCHAR", 0, 0))
    Assert.assertEquals(DecimalType(10, 5), SchemaUtils.getCatalystType("DECIMALV2", 10, 5))
    Assert.assertEquals(DataTypes.DoubleType, SchemaUtils.getCatalystType("TIME", 0, 0))
    Assert.assertEquals(DataTypes.StringType, SchemaUtils.getCatalystType("STRING", 0, 0))

    thrown.expect(classOf[DorisException])
    thrown.expectMessage(startsWith("Unsupported type"))
    SchemaUtils.getCatalystType("HLL", 0, 0)

    thrown.expect(classOf[DorisException])
    thrown.expectMessage(startsWith("Unrecognized Doris type"))
    SchemaUtils.getCatalystType("UNRECOGNIZED", 0, 0)
  }

  @Test
  def testConvertToSchema(): Unit = {
    val k1 = new TScanColumnDesc
    k1.setName("k1")
    k1.setType(TPrimitiveType.BOOLEAN)

    val k2 = new TScanColumnDesc
    k2.setName("k2")
    k2.setType(TPrimitiveType.DOUBLE)

    val expected = new Schema
    expected.setStatus(0)
    val ek1 = new Field("k1", "BOOLEAN", "", 0, 0, "")
    val ek2 = new Field("k2", "DOUBLE", "", 0, 0, "")
    expected.put(ek1)
    expected.put(ek2)

    Assert.assertEquals(expected, SchemaUtils.convertToSchema(Seq(k1, k2)))
  }
}
