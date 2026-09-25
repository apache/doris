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

package org.apache.doris.tools.ssb;

import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.VarcharType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Column order is the ssb-dbgen output order, also used by the existing SSB DDL. */
enum SsbTable {
    CUSTOMER("c", "c_custkey", "c_name:s", "c_address:s", "c_city:s", "c_nation:s", "c_region:s",
            "c_phone:s", "c_mktsegment:s"),
    PART("p", "p_partkey", "p_name:s", "p_mfgr:s", "p_category:s", "p_brand:s", "p_color:s",
            "p_type:s", "p_size", "p_container:s"),
    SUPPLIER("s", "s_suppkey", "s_name:s", "s_address:s", "s_city:s", "s_nation:s", "s_region:s", "s_phone:s"),
    DATES("d", "d_datekey", "d_date:s", "d_dayofweek:s", "d_month:s", "d_year", "d_yearmonthnum",
            "d_yearmonth:s", "d_daynuminweek", "d_daynuminmonth", "d_daynuminyear", "d_monthnuminyear",
            "d_weeknuminyear", "d_sellingseason:s", "d_lastdayinweekfl", "d_lastdayinmonthfl", "d_holidayfl",
            "d_weekdayfl"),
    LINEORDER("l", "lo_orderkey", "lo_linenumber", "lo_custkey", "lo_partkey", "lo_suppkey", "lo_orderdate",
            "lo_orderpriority:s", "lo_shippriority", "lo_quantity", "lo_extendedprice", "lo_ordtotalprice",
            "lo_discount", "lo_revenue", "lo_supplycost", "lo_tax", "lo_commitdate", "lo_shipmode:s");

    final String dbgenOption;
    final List<ColumnMetadata> columns;

    SsbTable(String dbgenOption, String... columns) {
        this.dbgenOption = dbgenOption;
        this.columns = Arrays.stream(columns).map(column -> column.endsWith(":s")
                ? new ColumnMetadata(column.substring(0, column.length() - 2), VarcharType.VARCHAR)
                : new ColumnMetadata(column, BigintType.BIGINT)).toList();
    }

    String tableName() {
        return name().toLowerCase(Locale.ROOT);
    }

    static SsbTable fromName(String name) {
        return valueOf(name.toUpperCase(Locale.ROOT));
    }
}
