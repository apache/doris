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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.transforms.SortOrderVisitor;

class DescribeSortOrderVisitor implements SortOrderVisitor<String> {

    public static final DescribeSortOrderVisitor INSTANCE = new DescribeSortOrderVisitor();

    private DescribeSortOrderVisitor() {}

    @Override
    public String field(
            String sourceName,
            int sourceId,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("%s %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String bucket(
            String sourceName,
            int sourceId,
            int numBuckets,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("bucket(%s, %s) %s %s", numBuckets, sourceName, direction, nullOrder);
    }

    @Override
    public String truncate(
            String sourceName,
            int sourceId,
            int width,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("truncate(%s, %s) %s %s", sourceName, width, direction, nullOrder);
    }

    @Override
    public String year(
            String sourceName,
            int sourceId,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("years(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String month(
            String sourceName,
            int sourceId,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("months(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String day(
            String sourceName,
            int sourceId,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("days(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String hour(
            String sourceName,
            int sourceId,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("hours(%s) %s %s", sourceName, direction, nullOrder);
    }

    @Override
    public String unknown(
            String sourceName,
            int sourceId,
            String transform,
            org.apache.iceberg.SortDirection direction,
            NullOrder nullOrder) {
        return String.format("%s(%s) %s %s", transform, sourceName, direction, nullOrder);
    }

}
