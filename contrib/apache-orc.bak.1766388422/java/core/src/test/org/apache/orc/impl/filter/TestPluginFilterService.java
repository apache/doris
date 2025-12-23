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

package org.apache.orc.impl.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.filter.BatchFilter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPluginFilterService {
  private final Configuration conf;

  public TestPluginFilterService() {
    conf = new Configuration();
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "aBcd");
    conf.set("my.filter.scope", "file://db/table1/.*");
  }

  @Test
  public void testFoundFilter() {
    conf.set("my.filter.name", "my_str_i_eq");
    assertNotNull(FilterFactory.findPluginFilters("file://db/table1/file1", conf));
  }

  @Test
  public void testErrorCreatingFilter() {
    Configuration localConf = new Configuration(conf);
    localConf.set("my.filter.name", "my_str_i_eq");
    localConf.set("my.filter.col.name", "");
    assertThrows(IllegalArgumentException.class,
                 () -> FilterFactory.findPluginFilters("file://db/table1/file1", localConf),
                 "Filter needs a valid column name");
  }

  @Test
  public void testMissingFilter() {
    assertTrue(FilterFactory.findPluginFilters("file://db/table11/file1", conf).isEmpty());
  }

  private Method getAllowedFilters() {
    Method method = null;
    try {
      method = FilterFactory.class.getDeclaredMethod("getAllowedFilters", List.class, List.class);
    } catch (NoSuchMethodException e) {
      assert(false);
    }
    method.setAccessible(true);
    return method;
  }

  @Test
  public void testHitAllowListFilter() throws Exception {
    conf.set("my.filter.name", "my_str_i_eq");
    // Hit the allowlist.
    List<String> allowListHit = new ArrayList<>();
    allowListHit.add("org.apache.orc.impl.filter.BatchFilterFactory$BatchFilterImpl");

    List<BatchFilter> pluginFilters = FilterFactory.findPluginFilters("file://db/table1/file1", conf);
    List<BatchFilter> allowListFilter = (List<BatchFilter>)getAllowedFilters().invoke(null, pluginFilters, allowListHit);

    assertEquals(1, allowListFilter.size());
  }

  @Test
  public void testAllowListFilterAllowAll() throws Exception {
    conf.set("my.filter.name", "my_str_i_eq");
    // Hit the allowlist.
    List<String> allowListHit = new ArrayList<>();
    allowListHit.add("*");

    List<BatchFilter> pluginFilters = FilterFactory.findPluginFilters("file://db/table1/file1", conf);
    List<BatchFilter> allowListFilter = (List<BatchFilter>)getAllowedFilters().invoke(null, pluginFilters, allowListHit);

    assertEquals(1, allowListFilter.size());
  }

  @Test
  public void testAllowListFilterDisallowAll() throws Exception {
    conf.set("my.filter.name", "my_str_i_eq");

    List<BatchFilter> pluginFilters = FilterFactory.findPluginFilters("file://db/table1/file1", conf);
    List<BatchFilter> allowListFilter = (List<BatchFilter>)getAllowedFilters().invoke(null, pluginFilters, new ArrayList<>());
    List<BatchFilter> allowListFilterWithNull = (List<BatchFilter>)getAllowedFilters().invoke(null, pluginFilters, null);

    assertEquals(0, allowListFilter.size());
    assertEquals(0, allowListFilterWithNull.size());
  }
}
