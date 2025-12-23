/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.util;

import org.apache.orc.OrcUtils;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Tests for OrcUtils.
 */
public class TestOrcUtils {

  @Test
  public void testBloomFilterIncludeColumns() {
    TypeDescription schema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imsi",  TypeDescription.createVarchar())
        .addField("imei", TypeDescription.createInt());

    boolean[] includeColumns = new boolean[3+1];
    includeColumns[1] = true;
    includeColumns[3] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn, imei", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_ACID() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("operation", TypeDescription.createString())
        .addField("originalTransaction", TypeDescription.createInt())
        .addField("bucket", TypeDescription.createInt())
        .addField("rowId", TypeDescription.createInt())
        .addField("currentTransaction", TypeDescription.createInt())
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[8+1];
    includeColumns[7] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_Nested() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[3+1];
    includeColumns[2] = true;

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("row.msisdn", schema));
  }

  @Test
  public void testBloomFilterIncludeColumns_NonExisting() {
    TypeDescription rowSchema = TypeDescription.createStruct()
        .addField("msisdn", TypeDescription.createString())
        .addField("imei", TypeDescription.createInt());

    TypeDescription schema = TypeDescription.createStruct()
        .addField("row", rowSchema);

    boolean[] includeColumns = new boolean[3+1];

    assertArrayEquals(includeColumns, OrcUtils.includeColumns("msisdn, row.msisdn2", schema));
  }
}
