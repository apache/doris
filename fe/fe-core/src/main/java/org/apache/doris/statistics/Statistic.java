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

package org.apache.doris.statistics;

public class Statistic {

    public Histogram histogram;

    public ColumnStatistic columnStatistic;

    public Statistic() {
    }

    public Statistic(Histogram histogram, ColumnStatistic columnStatistic) {
        this.histogram = histogram;
        this.columnStatistic = columnStatistic;
    }

    public Histogram getHistogram() {
        if (histogram != null) {
            return histogram;
        }
        return Histogram.DEFAULT;
    }

    public void setHistogram(Histogram histogram) {
        this.histogram = histogram;
    }

    public ColumnStatistic getColumnStatistic() {
        if (columnStatistic != null) {
            return columnStatistic;
        }
        return ColumnStatistic.DEFAULT;
    }

    public void setColumnStatistic(ColumnStatistic columnStatistic) {
        this.columnStatistic = columnStatistic;
    }
}
