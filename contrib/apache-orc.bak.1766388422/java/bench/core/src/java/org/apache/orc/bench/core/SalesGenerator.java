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

package org.apache.orc.bench.core;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.bench.core.convert.BatchReader;

public class SalesGenerator implements BatchReader {
  private final RandomGenerator generator;
  private long rowsRemaining;
  private static final double MOSTLY = 0.99999;

  public SalesGenerator(long rows) {
    this(rows, 42);
  }

  public SalesGenerator(long rows, int seed) {
    generator = new RandomGenerator(seed);
    // column 1
    generator.addField("sales_id", TypeDescription.Category.LONG)
        .addAutoIncrement(1000000000, 1);
    generator.addField("customer_id", TypeDescription.Category.LONG)
        .addIntegerRange(1000000000, 2000000000);
    generator.addField("col3", TypeDescription.Category.LONG)
        .addIntegerRange(1, 10000).hasNulls(0.9993100389335173);

    // column 4
    generator.addField("item_category", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000).hasNulls(0.00014784879996054823);
    generator.addField("item_count", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000);
    generator.addField("change_ts", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59");

    // column 7
    generator.addField("store_location", TypeDescription.Category.STRING)
        .addStringChoice("Los Angeles", "New York", "Cupertino", "Sunnyvale",
            "Boston", "Chicago", "Seattle", "Jackson",
            "Palo Alto", "San Mateo", "San Jose", "Santa Clara",
            "Irvine", "Torrance", "Gardena", "Hermosa", "Manhattan")
        .hasNulls(0.0004928293332019384);
    generator.addField("associate_id", TypeDescription.Category.STRING)
        .addStringPattern("MR V").hasNulls(0.05026859198659506);
    generator.addField("col9", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000000).hasNulls(MOSTLY);

    // column 10
    generator.addField("rebate_id", TypeDescription.Category.STRING)
        .addStringPattern("xxxxxx").hasNulls(MOSTLY);
    generator.addField("create_ts", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59");
    generator.addField("col13", TypeDescription.Category.LONG)
        .addIntegerRange(1, 100000).hasNulls(MOSTLY);

    // column 13
    generator.addField("size", TypeDescription.Category.STRING)
        .addStringChoice("Small", "Medium", "Large", "XL")
        .hasNulls(0.9503720861465674);
    generator.addField("col14", TypeDescription.Category.LONG)
        .addIntegerRange(1, 100000);
    generator.addField("fulfilled", TypeDescription.Category.BOOLEAN)
        .addBoolean();

    // column 16
    generator.addField("global_id", TypeDescription.Category.STRING)
        .addStringPattern("xxxxxxxxxxxxxxxx").hasNulls(0.021388793060962974);
    generator.addField("col17", TypeDescription.Category.STRING)
        .addStringPattern("L-xx").hasNulls(MOSTLY);
    generator.addField("col18", TypeDescription.Category.STRING)
        .addStringPattern("ll").hasNulls(MOSTLY);

    // column 19
    generator.addField("col19", TypeDescription.Category.LONG)
        .addIntegerRange(1, 100000);
    generator.addField("has_rebate", TypeDescription.Category.BOOLEAN)
        .addBoolean();
    RandomGenerator.Field list =
        generator.addField("col21",
        TypeDescription.fromString("array<struct<sub1:bigint,sub2:string," +
            "sub3:string,sub4:bigint,sub5:bigint,sub6:string>>"))
        .addList(0, 3)
        .hasNulls(MOSTLY);
    RandomGenerator.Field struct = list.getChildField(0).addStruct();
    struct.getChildField(0).addIntegerRange(0, 10000000);
    struct.getChildField(1).addStringPattern("VVVVV");
    struct.getChildField(2).addStringPattern("VVVVVVVV");
    struct.getChildField(3).addIntegerRange(0, 10000000);
    struct.getChildField(4).addIntegerRange(0, 10000000);
    struct.getChildField(5).addStringPattern("VVVVVVVV");

    // column 38
    generator.addField("vendor_id", TypeDescription.Category.STRING)
        .addStringPattern("Lxxxxxx").hasNulls(0.1870780148834459);
    generator.addField("country", TypeDescription.Category.STRING)
        .addStringChoice("USA", "Germany", "Ireland", "Canada", "Mexico",
            "Denmark").hasNulls(0.0004928293332019384);

    // column 40
    generator.addField("backend_version", TypeDescription.Category.STRING)
        .addStringPattern("X.xx").hasNulls(0.0005913951998423039);
    generator.addField("col41", TypeDescription.Category.LONG)
        .addIntegerRange(1000000000, 100000000000L);
    generator.addField("col42", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000000);

    // column 43
    generator.addField("col43", TypeDescription.Category.LONG)
        .addIntegerRange(1000000000, 10000000000L).hasNulls(0.9763934749396284);
    generator.addField("col44", TypeDescription.Category.LONG)
        .addIntegerRange(1, 100000000);
    generator.addField("col45", TypeDescription.Category.LONG)
        .addIntegerRange(1, 100000000);

    // column 46
    generator.addField("col46", TypeDescription.Category.LONG)
        .addIntegerRange(1, 10000000);
    generator.addField("col47", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000);
    generator.addField("col48", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000).hasNulls(MOSTLY);

    // column 49
    generator.addField("col49", TypeDescription.Category.STRING)
        .addStringPattern("xxxx").hasNulls(0.0004928293332019384);
    generator.addField("col50", TypeDescription.Category.STRING)
        .addStringPattern("ll").hasNulls(0.9496821250800848);
    generator.addField("col51", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000).hasNulls(0.9999014341333596);

    // column 52
    generator.addField("col52", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000).hasNulls(0.9980779656005125);
    generator.addField("col53", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000000);
    generator.addField("col54", TypeDescription.Category.LONG)
        .addIntegerRange(1,  1000000000);

    // column 55
    generator.addField("col55", TypeDescription.Category.STRING)
        .addStringChoice("X");
    generator.addField("col56", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59");
    generator.addField("col57", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59");

    // column 58
    generator.addField("md5", TypeDescription.Category.LONG)
        .addRandomInt();
    generator.addField("col59", TypeDescription.Category.LONG)
        .addIntegerRange(1000000000, 10000000000L);
    generator.addField("col69", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59")
        .hasNulls(MOSTLY);

    // column 61
    generator.addField("col61", TypeDescription.Category.STRING)
        .addStringPattern("X.xx").hasNulls(0.11399142476960233);
    generator.addField("col62", TypeDescription.Category.STRING)
        .addStringPattern("X.xx").hasNulls(0.9986200778670347);
    generator.addField("col63", TypeDescription.Category.TIMESTAMP)
        .addTimestampRange("2003-01-01 00:00:00", "2017-03-14 23:59:59");

    // column 64
    generator.addField("col64", TypeDescription.Category.LONG)
        .addIntegerRange(1, 1000000).hasNulls(MOSTLY);
    rowsRemaining = rows;
  }

  public boolean nextBatch(VectorizedRowBatch batch) {
    int rows = (int) Math.min(batch.getMaxSize(), rowsRemaining);
    generator.generate(batch, rows);
    rowsRemaining -= rows;
    return rows != 0;
  }

  @Override
  public void close() {
    // PASS
  }

  public TypeDescription getSchema() {
    return generator.getSchema();
  }

  public static void main(String[] args) throws Exception {
    SalesGenerator sales = new SalesGenerator(10, 42);
    System.out.println("Schema " + sales.getSchema());
  }
}
