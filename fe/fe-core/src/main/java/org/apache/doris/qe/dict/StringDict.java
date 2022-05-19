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

import java.util.List;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.Config;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InternalQueryExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TColumnDict;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TThriftIPCRowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.google.common.collect.Lists;

public class StringDict extends IDict {
	private static final Logger LOG = LogManager.getLogger(StringDict.class);

	private List<String> dictValues;

	public StringDict(long dictId, long dbId, long tableId, String columnName) {
		super(dictId, dbId, tableId, columnName);
		dictValues = Lists.newArrayList();
	}

	@Override
	public void initDict() {

	}
	
	@Override
	public int getDictElemNum() {
		return this.dictValues.size();
	}

	@Override
	public void mergeWith(IDict dict) {
		// do nothing, not implement it yet
	}

	@Override
	public IDict doRefresh() {
		Catalog catalog = Catalog.getCurrentCatalog();
		ConnectContext connectContext = new ConnectContext();
		connectContext.setInternalQuery(true);
		connectContext.setCatalog(catalog);
		connectContext.setCluster(SystemInfoService.DEFAULT_CLUSTER);
		connectContext.setCurrentUserIdentity(UserIdentity.ROOT);
		connectContext.setQualifiedUser(PaloAuth.ROOT_USER);
		Database db = catalog.getDbNullable(getDbId());
		if (db == null) {
			LOG.warn("could not find database with id = {} from catalog", getDbId());
			return null;
		}
		connectContext.setDatabase(ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, db.getFullName()));
		Table tbl = db.getTableNullable(getTableId());
		if (tbl == null) {
			LOG.warn("could not find table with id = {} from database {}", getTableId(), db.getFullName());
		}
		connectContext.setThreadLocalInfo();
		connectContext.getSessionVariable().setEnableVectorizedEngine(true);
		String stmt = "select distinct "+ getColumnName() + " from " + tbl.getName() + "[META];";
		LOG.info("try to use query stmt {} to get string dict", stmt);
		InternalQueryExecutor queryExecutor = new InternalQueryExecutor(connectContext, stmt);
		List<String> updatedDictValues = Lists.newArrayList();
		boolean valid = true;
		try {
			queryExecutor.execute();
			while (valid) {
				TResultBatch resultBatch = queryExecutor.getNext();
				// The last batch is null, to indicate the end
				if (resultBatch == null) {
					break;
				}
				TThriftIPCRowBatch rowBatch = resultBatch.getThriftRowBatch();
				if (rowBatch == null) {
					break;
				}
				for (int i = 0; i < rowBatch.getNumRows(); ++i) {
					updatedDictValues.add(rowBatch.cols.get(0).string_vals.get(i));
					if (updatedDictValues.size() > Config.max_string_dict_size) {
						LOG.info("too many dict received larger than limit {}, skip build dict", Config.max_string_dict_size);
						invalidDict(false);
						valid = false;
						break;
					}
				}
			}
		} catch (Exception e) {
			valid = false;
			LOG.info("errors while execute query ", e);
		}
		if (!valid) {
			queryExecutor.cancel();
			return null;
		}
		StringDict newDict = new StringDict(getDictId(), getDbId(), getTableId(), getColumnName());
		newDict.dictValues.addAll(updatedDictValues);
		return newDict;
	}

	@Override
	public TColumnDict toThrift() {
		TColumnDict columnDict = new TColumnDict();
		columnDict.setType(TPrimitiveType.VARCHAR);
		columnDict.setStrDict(dictValues);
		return columnDict;
	}

	@Override
	public String toString() {
		return "StringDict [dictValues=" + String.join(",", dictValues) + "]";
	}

}
