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

package org.apache.orc.bench.core.filter;

import org.apache.orc.impl.filter.RowFilterFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.Reader;
import org.apache.orc.impl.filter.FilterFactory;
import org.apache.orc.impl.OrcFilterContextImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestFilter {
  private static final Logger LOG = LoggerFactory.getLogger(TestFilter.class);
  private static final long seed = 1024;
  protected final Configuration conf = new Configuration();
  protected final Random rnd = new Random(seed);
  protected final VectorizedRowBatch b = FilterBenchUtil.createBatch(rnd);
  protected final OrcFilterContextImpl fc = (OrcFilterContextImpl)
    new OrcFilterContextImpl(FilterBenchUtil.schema, false).setBatch(b);

  public static Stream<Arguments> filters() {
    return Stream.of(
      arguments("simple", "row", false),
      arguments("simple", "vector", false),
      arguments("complex", "row", true),
      arguments("complex", "vector", true),
      arguments("complex", "row", false),
      arguments("complex", "vector", false)
    );
  }

  @BeforeEach
  public void setup() {
    FilterBenchUtil.unFilterBatch(fc);
  }

  @ParameterizedTest(name = "#{index} - {0}+{1}")
  @MethodSource("org.apache.orc.bench.core.filter.TestFilter#filters")
  public void testFilter(String complexity, String filterType, boolean normalize)
    throws FilterFactory.UnSupportedSArgException {
    new Filter(complexity, filterType, normalize).execute();
  }

  private class Filter {
    protected final SearchArgument sArg;
    protected final int[] expSel;
    protected final Consumer<OrcFilterContext> filter;

    private Filter(String complexity, String filterType, boolean normalize)
      throws FilterFactory.UnSupportedSArgException {
      Map.Entry<SearchArgument, int[]> ft;
      switch (complexity) {
        case "simple":
          ft = FilterBenchUtil.createSArg(new Random(seed), b, 5);
          break;
        case "complex":
          ft = FilterBenchUtil.createComplexSArg(new Random(seed), b, 10, 8);
          break;
        default:
          throw new IllegalArgumentException();
      }
      sArg = ft.getKey();
      LOG.info("SearchArgument has {} expressions", sArg.getExpression().getChildren().size());
      expSel = ft.getValue();

      switch (filterType) {
        case "row":
          filter = RowFilterFactory.create(sArg,
                                           FilterBenchUtil.schema,
                                           OrcFile.Version.CURRENT,
                                           normalize);
          break;
        case "vector":
          Reader.Options options = new Reader.Options(conf)
            .searchArgument(sArg, new String[0])
            .allowSARGToFilter(true);
          filter = FilterFactory.createBatchFilter(options,
                                                   FilterBenchUtil.schema,
                                                   false,
                                                   OrcFile.Version.CURRENT,
                                                   normalize,
                                                   null,
                                                   null);
          break;
        default:
          throw new IllegalArgumentException();
      }
    }

    private void execute() {
      filter.accept(fc.setBatch(b));
      FilterBenchUtil.validate(fc, expSel);
    }
  }
}