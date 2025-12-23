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

import org.apache.hadoop.hive.ql.io.sarg.ExpressionTree;
import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFilterContext;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.filter.leaf.LeafFilterFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

public class RowFilterFactory {
  public static Consumer<OrcFilterContext> create(SearchArgument sArg,
                                           TypeDescription readSchema,
                                           OrcFile.Version version,
                                           boolean normalize)
    throws FilterFactory.UnSupportedSArgException {
    Set<String> colIds = new HashSet<>();
    ExpressionTree expr = normalize ? sArg.getExpression() : sArg.getCompactExpression();
    RowFilter filter = create(expr,
                              colIds,
                              sArg.getLeaves(),
                              readSchema,
                              version);
    return new RowBatchFilter(filter, colIds.toArray(new String[0]));
  }

  static RowFilter create(ExpressionTree expr,
                                 Set<String> colIds,
                                 List<PredicateLeaf> leaves,
                                 TypeDescription readSchema,
                                 OrcFile.Version version)
    throws FilterFactory.UnSupportedSArgException {
    RowFilter result;
    switch (expr.getOperator()) {
      case OR:
        RowFilter[] orFilters = new RowFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          orFilters[i] = create(expr.getChildren().get(i), colIds, leaves, readSchema, version);
        }
        result = new RowFilter.OrFilter(orFilters);
        break;
      case AND:
        RowFilter[] andFilters = new RowFilter[expr.getChildren().size()];
        for (int i = 0; i < expr.getChildren().size(); i++) {
          andFilters[i] = create(expr.getChildren().get(i), colIds, leaves, readSchema, version);
        }
        result = new RowFilter.AndFilter(andFilters);
        break;
      case LEAF:
        result = createLeafFilter(leaves.get(expr.getLeaf()), colIds, readSchema, version, false);
        break;
      default:
        throw new FilterFactory.UnSupportedSArgException(String.format(
          "SArg Expression: %s is not supported",
          expr));
    }
    return result;
  }

  private static RowFilter createLeafFilter(PredicateLeaf leaf,
                                            Set<String> colIds,
                                            TypeDescription readSchema,
                                            OrcFile.Version version,
                                            boolean negated)
    throws FilterFactory.UnSupportedSArgException {
    colIds.add(leaf.getColumnName());
    LeafFilter f = (LeafFilter) LeafFilterFactory.createLeafVectorFilter(leaf,
                                                                         colIds,
                                                                         readSchema,
                                                                         false,
                                                                         version,
                                                                         negated);
    return new RowFilter.LeafFilter(f);
  }

  static class RowBatchFilter implements Consumer<OrcFilterContext> {

    private final RowFilter filter;
    private final String[] colNames;

    private RowBatchFilter(RowFilter filter, String[] colNames) {
      this.filter = filter;
      this.colNames = colNames;
    }

    @Override
    public void accept(OrcFilterContext batch) {
      int size = 0;
      int[] selected = batch.getSelected();

      for (int i = 0; i < batch.getSelectedSize(); i++) {
        if (filter.accept(batch, i)) {
          selected[size] = i;
          size += 1;
        }
      }
      batch.setSelectedInUse(true);
      batch.setSelected(selected);
      batch.setSelectedSize(size);
    }

    public String[] getColNames() {
      return colNames;
    }
  }
}
