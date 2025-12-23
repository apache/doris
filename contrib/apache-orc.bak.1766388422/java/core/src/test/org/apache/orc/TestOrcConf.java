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
package org.apache.orc;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestOrcConf {

  // Long
  public final OrcConf ORC_STRIPE_SIZE_CONF = OrcConf.STRIPE_SIZE;
  // Boolean
  public final OrcConf ORC_ENABLE_INDEXES_CONF = OrcConf.ENABLE_INDEXES;
  // int
  public final OrcConf ORC_BLOCK_SIZE_CONF = OrcConf.ENABLE_INDEXES;
  // String
  public final OrcConf ORC_COMPRESS_CONF = OrcConf.COMPRESS;
  // Double
  public final OrcConf ORC_BLOOM_FILTER_FPP_CONF = OrcConf.BLOOM_FILTER_FPP;

  @Test
  public void testLoadConfFromProperties() {
    Properties tableProperties = new Properties();
    Configuration conf = new Configuration();

    tableProperties.setProperty(ORC_STRIPE_SIZE_CONF.getAttribute(), "1000");
    conf.setInt(ORC_STRIPE_SIZE_CONF.getAttribute(), 2000);
    assertEquals(1000, ORC_STRIPE_SIZE_CONF.getLong(tableProperties, conf));

    tableProperties.setProperty(ORC_BLOCK_SIZE_CONF.getAttribute(), "1000");
    conf.setInt(ORC_BLOCK_SIZE_CONF.getAttribute(), 2000);
    assertEquals(1000, ORC_BLOCK_SIZE_CONF.getInt(tableProperties, conf));

    tableProperties.setProperty(ORC_ENABLE_INDEXES_CONF.getAttribute(), "true");
    conf.setBoolean(ORC_ENABLE_INDEXES_CONF.getAttribute(), false);
    assertTrue(ORC_ENABLE_INDEXES_CONF.getBoolean(tableProperties, conf));

    tableProperties.setProperty(ORC_COMPRESS_CONF.getAttribute(), "snappy2");
    conf.set(ORC_COMPRESS_CONF.getAttribute(), "snappy3");
    assertEquals("snappy2", ORC_COMPRESS_CONF.getString(tableProperties, conf));

    tableProperties.setProperty(ORC_BLOOM_FILTER_FPP_CONF.getAttribute(), "0.5");
    conf.setDouble(ORC_BLOOM_FILTER_FPP_CONF.getAttribute(), 0.4);
    assertEquals(0.5, ORC_BLOOM_FILTER_FPP_CONF.getDouble(tableProperties, conf));
  }

  @Test
  public void testLoadConfFromConfiguration() {
    Properties tableProperties = new Properties();
    Configuration conf = new Configuration();

    conf.setInt(ORC_STRIPE_SIZE_CONF.getAttribute(), 2000);
    assertEquals(2000, ORC_STRIPE_SIZE_CONF.getLong(tableProperties, conf));

    conf.setInt(ORC_BLOCK_SIZE_CONF.getAttribute(), 2000);
    assertEquals(2000, ORC_BLOCK_SIZE_CONF.getInt(tableProperties, conf));

    conf.setBoolean(ORC_ENABLE_INDEXES_CONF.getAttribute(), false);
    assertFalse(ORC_ENABLE_INDEXES_CONF.getBoolean(tableProperties, conf));

    conf.set(ORC_COMPRESS_CONF.getAttribute(), "snappy3");
    assertEquals("snappy3", ORC_COMPRESS_CONF.getString(tableProperties, conf));

    conf.setDouble(ORC_BLOOM_FILTER_FPP_CONF.getAttribute(), 0.4);
    assertEquals(0.4, ORC_BLOOM_FILTER_FPP_CONF.getDouble(tableProperties, conf));
  }

  @Test
  public void testDefaultValue() {
    Properties tableProperties = new Properties();
    Configuration conf = new Configuration();

    for (OrcConf orcConf : OrcConf.values()) {
      if (orcConf.getDefaultValue() instanceof String) {
        assertEquals(orcConf.getString(tableProperties, conf), orcConf.getDefaultValue());
      }
      if (orcConf.getDefaultValue() instanceof Long) {
        assertEquals(orcConf.getLong(tableProperties, conf), orcConf.getDefaultValue());
      }
      if (orcConf.getDefaultValue() instanceof Integer) {
        assertEquals(orcConf.getInt(tableProperties, conf), orcConf.getDefaultValue());
      }
      if (orcConf.getDefaultValue() instanceof Boolean) {
        assertEquals(orcConf.getBoolean(tableProperties, conf), orcConf.getDefaultValue());
      }
      if (orcConf.getDefaultValue() instanceof Double) {
        assertEquals(orcConf.getDouble(tableProperties, conf), orcConf.getDefaultValue());
      }
    }
  }

  @Test
  public void testSetValue() {
    Properties tableProperties = new Properties();
    Configuration conf = new Configuration();

    ORC_STRIPE_SIZE_CONF.setLong(conf, 1000);
    assertEquals(1000, ORC_STRIPE_SIZE_CONF.getLong(tableProperties, conf));

    ORC_BLOCK_SIZE_CONF.setInt(conf, 2000);
    assertEquals(2000, ORC_BLOCK_SIZE_CONF.getLong(tableProperties, conf));

    ORC_ENABLE_INDEXES_CONF.setBoolean(conf, false);
    assertFalse(ORC_ENABLE_INDEXES_CONF.getBoolean(tableProperties, conf));

    ORC_COMPRESS_CONF.setString(conf, "snappy3");
    assertEquals("snappy3", ORC_COMPRESS_CONF.getString(tableProperties, conf));

    ORC_COMPRESS_CONF.setDouble(conf, 0.4);
    assertEquals(0.4, ORC_COMPRESS_CONF.getDouble(tableProperties, conf));
  }

  @Test
  public void testGetHiveConfValue() {
    Properties tableProperties = new Properties();
    Configuration conf = new Configuration();

    conf.setInt(ORC_STRIPE_SIZE_CONF.getHiveConfName(), 2000);
    assertEquals(2000, ORC_STRIPE_SIZE_CONF.getLong(tableProperties, conf));
  }

  @Test
  public void testGetStringAsList() {
    Configuration conf = new Configuration();

    conf.set(ORC_COMPRESS_CONF.getHiveConfName(), "a,,b,c, ,d,");
    List<String> valueList1 =  ORC_COMPRESS_CONF.getStringAsList(conf);
    assertEquals(valueList1.size(), 4);
    assertEquals(valueList1.get(0), "a");
    assertEquals(valueList1.get(1), "b");
    assertEquals(valueList1.get(2), "c");
    assertEquals(valueList1.get(3), "d");

    conf.set(ORC_COMPRESS_CONF.getHiveConfName(), "");
    List<String> valueList2 =  ORC_COMPRESS_CONF.getStringAsList(conf);
    assertEquals(valueList2.size(), 0);

    conf.set(ORC_COMPRESS_CONF.getHiveConfName(), " abc,  efg,  ");
    List<String> valueList3 =  ORC_COMPRESS_CONF.getStringAsList(conf);
    assertEquals(valueList3.size(), 2);
    assertEquals(valueList3.get(0), "abc");
    assertEquals(valueList3.get(1), "efg");
  }
}
