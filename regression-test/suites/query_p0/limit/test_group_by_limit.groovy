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

suite("test_group_by_limit", "query") {

sql 'set enable_agg_spill=false'

sql 'set enable_force_spill=false'

sql 'set topn_opt_limit_threshold=10'


// different types
qt_select """ select  sum(orderkey), count(partkey), shipmode from tpch_tiny_lineitem group by shipmode limit 3; """

qt_select """ select  sum(orderkey), count(partkey),  linenumber from tpch_tiny_lineitem group by linenumber limit 3; """

qt_select """ select  sum(orderkey), count(partkey),  tax from tpch_tiny_lineitem group by tax limit 3; """

qt_select """ select  sum(orderkey), count(partkey),  commitdate from tpch_tiny_lineitem group by commitdate limit 3; """


// group by functions
qt_select """ select  sum(orderkey), count(partkey),  cast(commitdate as datetime) from tpch_tiny_lineitem group by cast(commitdate as datetime) limit 3; """

qt_select """ select  sum(orderkey), count(partkey),  month(commitdate) from tpch_tiny_lineitem group by month(commitdate) limit 3; """


// mutli column
qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber from tpch_tiny_lineitem group by shipmode, linenumber limit 3; """

qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax from tpch_tiny_lineitem group by shipmode, linenumber, tax limit 3; """

qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax , commitdate from tpch_tiny_lineitem group by shipmode, linenumber, tax, commitdate  limit 3; """


// group by + order by 

// group by columns eq order by columns
qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax from tpch_tiny_lineitem group by shipmode, linenumber, tax order by shipmode, linenumber, tax limit 3; """

// group by columns contains order by columns
qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax from tpch_tiny_lineitem group by shipmode, linenumber, tax order by shipmode limit 3; """

// desc order by column
qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax from tpch_tiny_lineitem group by shipmode, linenumber, tax order by shipmode desc, linenumber, tax limit 3; """

qt_select """ select  sum(orderkey), count(partkey), shipmode, linenumber , tax from tpch_tiny_lineitem group by shipmode, linenumber, tax order by shipmode desc, linenumber, tax desc limit 3; """

}
