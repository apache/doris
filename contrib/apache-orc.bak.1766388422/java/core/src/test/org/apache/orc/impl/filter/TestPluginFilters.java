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
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgumentFactory;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.BatchFilter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPluginFilters extends ATestFilter {

  @Test
  public void testPluginFilterWithSArg() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "B", "c", "dE", "e", "f"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, true);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "de");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 4L, 6L)
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f1"})
      .allowSARGToFilter(true);
    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table1/file1",
                                                    conf);
    assertTrue(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should be an AND Batch filter");
    assertArrayEquals(new String[] {"f1", "f2"}, f.getColumnNames());
    f.accept(fc.setBatch(batch));
    validateSelected(3);
  }

  @Test
  public void testPluginSelectsNone() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "B", "c", "dE", "e", "f"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, true);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "g");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 4L, 6L)
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f1"})
      .allowSARGToFilter(true);
    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table1/file1",
                                                    conf);
    assertTrue(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should be an AND Batch filter");
    f.accept(fc.setBatch(batch));
    validateNoneSelected();
  }

  @Test
  public void testPluginDisabled() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "B", "c", "dE", "e", "f"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, false);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "g");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 4L, 6L)
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f1"})
      .allowSARGToFilter(true);
    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table1/file1",
                                                    conf);
    assertFalse(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should not be an AND Batch filter");
    f.accept(fc.setBatch(batch));
    validateSelected(1, 3, 5);
  }

  @Test
  public void testPluginNonMatchingPath() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"a", "B", "c", "dE", "e", "f"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, true);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "g");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 4L, 6L)
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f1"})
      .allowSARGToFilter(true);
    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table2/file1",
                                                    conf);
    assertFalse(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should not be an AND Batch filter");
    f.accept(fc.setBatch(batch));
    validateSelected(1, 3, 5);
  }

  @Test
  public void testPluginSelectsAll() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"abcdef", "Abcdef", "aBcdef", null, "abcDef", "abcdEf"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, true);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "abcdef");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f1", PredicateLeaf.Type.LONG, 2L, 4L, 6L)
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f1"})
      .allowSARGToFilter(true);
    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table1/file1",
                                                    conf);
    assertTrue(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should be an AND Batch filter");
    f.accept(fc.setBatch(batch));
    validateSelected(1, 5);
  }

  @Test
  public void testPluginSameColumn() {
    setBatch(new Long[] {1L, 2L, null, 4L, 5L, 6L},
             new String[] {"abcdef", "Abcdef", "aBcdef", null, "abcDef", "abcdEf"});

    // Define the plugin filter
    Configuration conf = new Configuration();
    OrcConf.ALLOW_PLUGIN_FILTER.setBoolean(conf, true);
    conf.set("my.filter.name", "my_str_i_eq");
    conf.set("my.filter.col.name", "f2");
    conf.set("my.filter.col.value", "abcdef");
    conf.set("my.filter.scope", "file://db/table1/.*");

    SearchArgument sArg = SearchArgumentFactory.newBuilder()
      .in("f2", PredicateLeaf.Type.STRING, "Abcdef", "abcdEf")
      .build();

    // Setup Options
    Reader.Options opts = new Reader.Options(conf)
      .searchArgument(sArg, new String[] {"f2"})
      .allowSARGToFilter(true);

    BatchFilter f = FilterFactory.createBatchFilter(opts,
                                                    schema,
                                                    false,
                                                    OrcFile.Version.CURRENT,
                                                    false,
                                                    "file://db/table1/file1",
                                                    conf);
    assertTrue(f instanceof BatchFilterFactory.AndBatchFilterImpl,
               "Filter should be an AND Batch filter");
    assertArrayEquals(new String[] {"f2"}, f.getColumnNames());
    f.accept(fc.setBatch(batch));
    validateSelected(1, 5);
  }
}
