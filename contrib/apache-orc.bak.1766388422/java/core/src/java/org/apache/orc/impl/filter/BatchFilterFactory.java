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

import org.apache.orc.OrcFilterContext;
import org.apache.orc.filter.BatchFilter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Provides an abstraction layer between the VectorFilters and the
 * Consumer&lt;OrcFilterContext&gt;.
 */
class BatchFilterFactory {
  static BatchFilter create(List<BatchFilter> filters) {
    if (filters.isEmpty()) {
      return null;
    } else if (filters.size() == 1) {
      return filters.get(0);
    } else {
      return new AndBatchFilterImpl(filters.toArray(new BatchFilter[0]));
    }
  }

  static BatchFilter create(Consumer<OrcFilterContext> filter,
                                   String[] colNames) {
    return filter instanceof BatchFilter ? (BatchFilter) filter
        : new WrappedFilterImpl(filter, colNames);
  }

  static BatchFilter create(VectorFilter filter, String[] colNames) {
    return new BatchFilterImpl(filter, colNames);
  }

  /**
   * Use to wrap the VectorFilter for application by the BatchReader
   */
  private static class BatchFilterImpl implements BatchFilter {
    final VectorFilter filter;
    private final String[] colNames;
    private final Selected bound = new Selected();
    private final Selected selOut = new Selected();

    private BatchFilterImpl(VectorFilter filter, String[] colNames) {
      this.filter = filter;
      this.colNames = colNames;
    }

    @Override
    public void accept(OrcFilterContext fc) {
      // Define the bound to be the batch size
      bound.initialize(fc);
      // selOut is set to the selectedVector
      selOut.sel = fc.getSelected();
      selOut.selSize = 0;
      filter.filter(fc, bound, selOut);

      if (selOut.selSize < fc.getSelectedSize()) {
        fc.setSelectedSize(selOut.selSize);
        fc.setSelectedInUse(true);
      } else if (selOut.selSize > fc.getSelectedSize()) {
        throw new RuntimeException(
          String.format("Unexpected state: Filtered size %s > input size %s",
                        selOut.selSize, fc.getSelectedSize()));
      }
    }

    @Override
    public String[] getColumnNames() {
      return colNames;
    }
  }

  static class AndBatchFilterImpl implements BatchFilter {
    private final BatchFilter[] filters;
    private final String[] colNames;

    AndBatchFilterImpl(BatchFilter... filters) {
      this.filters = filters;
      Set<String> names = new HashSet<>();
      for (BatchFilter filter : this.filters) {
        names.addAll(Arrays.asList(filter.getColumnNames()));
      }
      this.colNames = names.toArray(new String[0]);
    }

    @Override
    public void accept(OrcFilterContext fc) {
      for (int i = 0; fc.getSelectedSize() > 0 && i < filters.length; i++) {
        filters[i].accept(fc);
      }
    }

    @Override
    public String[] getColumnNames() {
      return colNames;
    }

  }

  private static class WrappedFilterImpl implements BatchFilter {
    private final Consumer<OrcFilterContext> filter;
    private final String[] colNames;

    private WrappedFilterImpl(Consumer<OrcFilterContext> filter, String[] colNames) {
      this.filter = filter;
      this.colNames = colNames;
    }

    @Override
    public String[] getColumnNames() {
      return colNames;
    }

    @Override
    public void accept(OrcFilterContext filterContext) {
      filter.accept(filterContext);
    }
  }
}
