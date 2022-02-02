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

package org.apache.doris.tablefunction;

import java.util.ArrayList;
import java.util.List;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.planner.ScanNode;

// Table function that generate int64 numbers
// have a single column number
public class TableFunctionNumbers extends TableFunction {

	public static final String NAME = "numbers";
	
	public TableFunctionNumbers(List<String> params) {
		// Only have a single parameter Number of rows to generate
	}

	@Override
	public String getTableName() {
		return "TableFunctionNumbers";
	}

	@Override
	public List<Column> getTableColumns() {
		List<Column> resColumns = new ArrayList<>();
		resColumns.add(new Column("number", PrimitiveType.BIGINT, false));
		return resColumns;
	}

	@Override
	public ScanNode getScanNode() {
		// TODO Auto-generated method stub
		return null;
	}

}
