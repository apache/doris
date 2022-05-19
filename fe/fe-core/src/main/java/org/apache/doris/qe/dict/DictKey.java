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

package org.apache.doris.qe.dict;

import java.util.Objects;

public class DictKey {

	private long dbId;
	private long tableId;
	private String columnName;
	
	public DictKey(long dbId, long tableId, String columnName) {
		this.dbId = dbId;
		this.tableId = tableId;
		this.columnName = columnName;
	}
	
	public long getTableId() {
		return tableId;
	}
	
	public String getColumnName() {
		return columnName;
	}

	public long getDbId() {
		return dbId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(columnName, dbId, tableId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DictKey other = (DictKey) obj;
		return Objects.equals(columnName, other.columnName) && dbId == other.dbId && tableId == other.tableId;
	}

	@Override
	public String toString() {
		return "DictKey [dbId=" + dbId + ", tableId=" + tableId + ", columnName=" + columnName + "]";
	}
}
