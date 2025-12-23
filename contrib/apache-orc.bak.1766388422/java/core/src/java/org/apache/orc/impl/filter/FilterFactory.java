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
import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.filter.BatchFilter;
import org.apache.orc.filter.PluginFilterService;
import org.apache.orc.impl.filter.leaf.LeafFilterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;

public class FilterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FilterFactory.class);

  /**
   * Create a BatchFilter. This considers both the input filter and the SearchArgument filter. If
   * both are available then they are compounded by AND.
   *
   * @param opts              for reading the file
   * @param readSchema        that should be used
   * @param isSchemaCaseAware identifies if the schema is case-sensitive
   * @param version           provides the ORC file version
   * @param normalize         identifies if the SArg should be normalized or not
   * @param filePath          that is fully qualified to determine plugin filter(s)
   * @param conf              configuration shared when determining Plugin filter(s)
   * @return BatchFilter that represents the SearchArgument or null
   */
  public static BatchFilter createBatchFilter(Reader.Options opts,
                                              TypeDescription readSchema,
                                              boolean isSchemaCaseAware,
                                              OrcFile.Version version,
                                              boolean normalize,
                                              String filePath,
                                              Configuration conf) {
    List<BatchFilter> filters = new ArrayList<>(2);

    // 1. Process input filter
    if (opts.getFilterCallback() != null) {
      filters.add(BatchFilterFactory.create(opts.getFilterCallback(),
                                            opts.getPreFilterColumnNames()));
    }

    // 2. Process PluginFilter
    if (opts.allowPluginFilters()) {
      List<BatchFilter> pluginFilters = findPluginFilters(filePath, conf);
      pluginFilters = getAllowedFilters(pluginFilters, opts.pluginAllowListFilters());
      if (!pluginFilters.isEmpty()) {
        LOG.debug("Added plugin filters {} to the read", pluginFilters);
        filters.addAll(pluginFilters);
      }
    }

    // 3. Process SArgFilter
    if (opts.isAllowSARGToFilter() && opts.getSearchArgument() != null) {
      SearchArgument sArg = opts.getSearchArgument();
      Set<String> colNames = new HashSet<>();
      try {
        ExpressionTree exprTree = normalize ? sArg.getExpression() : sArg.getCompactExpression();
        LOG.debug("normalize={}, using expressionTree={}", normalize, exprTree);
        filters.add(BatchFilterFactory.create(createSArgFilter(exprTree,
                                                               colNames,
                                                               sArg.getLeaves(),
                                                               readSchema,
                                                               isSchemaCaseAware,
                                                               version),
                                              colNames.toArray(new String[0])));
      } catch (UnSupportedSArgException e) {
        LOG.warn("SArg: {} is not supported\n{}", sArg, e.getMessage());
      }
    }

    return BatchFilterFactory.create(filters);
  }

  public static VectorFilter createSArgFilter(ExpressionTree expr,
                                              Set<String> colIds,
                                              List<PredicateLeaf> leaves,
                                              TypeDescription readSchema,
                                              boolean isSchemaCaseAware,
                                              OrcFile.Version version)
    throws UnSupportedSArgException {
    VectorFilter result;
    switch (expr.getOperator()) {
      case OR:
        VectorFilter[] orFilters = new VectorFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          orFilters[i] = createSArgFilter(expr.getChildren().get(i),
                                          colIds,
                                          leaves,
                                          readSchema,
                                          isSchemaCaseAware,
                                          version);
        }
        result = new OrFilter(orFilters);
        break;
      case AND:
        VectorFilter[] andFilters = new VectorFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          andFilters[i] = createSArgFilter(expr.getChildren().get(i),
                                           colIds,
                                           leaves,
                                           readSchema,
                                           isSchemaCaseAware,
                                           version);
        }
        result = new AndFilter(andFilters);
        break;
      case NOT:
        // Not is expected to be pushed down that it only happens on leaf filters
        ExpressionTree leaf = expr.getChildren().get(0);
        assert leaf.getOperator() == ExpressionTree.Operator.LEAF;
        result = LeafFilterFactory.createLeafVectorFilter(leaves.get(leaf.getLeaf()),
                                                          colIds,
                                                          readSchema,
                                                          isSchemaCaseAware,
                                                          version,
                                                          true);
        break;
      case LEAF:
        result = LeafFilterFactory.createLeafVectorFilter(leaves.get(expr.getLeaf()),
                                                          colIds,
                                                          readSchema,
                                                          isSchemaCaseAware,
                                                          version,
                                                          false);
        break;
      default:
        throw new UnSupportedSArgException(String.format("SArg expression: %s is not supported",
                                                         expr));
    }
    return result;
  }

  public static class UnSupportedSArgException extends Exception {

    public UnSupportedSArgException(String message) {
      super(message);
    }
  }

  /**
   * Find filter(s) for a given file path. The order in which the filter services are invoked is
   * unpredictable.
   *
   * @param filePath fully qualified path of the file being evaluated
   * @param conf     reader configuration of ORC, can be used to configure the filter services
   * @return The plugin filter(s) matching the given file, can be empty if none are found
   */
  static List<BatchFilter> findPluginFilters(String filePath, Configuration conf) {
    List<BatchFilter> filters = new ArrayList<>();
    for (PluginFilterService s : ServiceLoader.load(PluginFilterService.class)) {
      LOG.debug("Processing filter service {}", s);
      BatchFilter filter = s.getFilter(filePath, conf);
      if (filter != null) {
        filters.add(filter);
      }
    }
    return filters;
  }

  /**
   * Filter BatchFilter which is in the allowList.
   *
   * @param filters whole BatchFilter list we load from class path.
   * @param allowList a Class-Name list that we want to load in.
   */
  private static List<BatchFilter> getAllowedFilters(
      List<BatchFilter> filters, List<String> allowList) {
    List<BatchFilter> allowBatchFilters = new ArrayList<>();

    if (allowList != null && allowList.contains("*")) {
      return filters;
    }

    if (allowList == null || allowList.isEmpty() || filters == null) {
      LOG.debug("Disable all PluginFilter.");
      return allowBatchFilters;
    }

    for (BatchFilter filter: filters) {
      if (allowList.contains(filter.getClass().getName())) {
        allowBatchFilters.add(filter);
      } else {
        LOG.debug("Ignoring filter service {}", filter);
      }
    }
    return allowBatchFilters;
  }
}
