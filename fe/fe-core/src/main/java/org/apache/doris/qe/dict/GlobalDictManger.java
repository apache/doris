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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.google.common.collect.Lists;
import com.alibaba.google.common.collect.Maps;

public class GlobalDictManger extends Daemon {
	private static final Logger LOG = LogManager.getLogger(GlobalDictManger.class);
	private Map<DictKey, IDict> dictsMap = null;
	private ReentrantReadWriteLock lock;
	private WriteLock writeLock;
	private ReadLock readLock;
	private AtomicLong dictIdGenerator;

	public GlobalDictManger() {
		super("GLOBAL_DICT_MGR", Config.dict_check_interval_sec * 1000L);
		this.dictsMap = Maps.newConcurrentMap();
		this.lock = new ReentrantReadWriteLock();
		this.writeLock = this.lock.writeLock();
		this.readLock = this.lock.readLock();
		this.dictIdGenerator = new AtomicLong(1);
	}

	// During query, if dict is invalid, then optimizer should not use it any more
	public IDict getDictForQuery(long dbId, long tableId, String column) {
		IDict dict = this.dictsMap.get(new DictKey(dbId, tableId, column));
		if (dict == null) {
			return null;
		}
		if (dict.getState() != DictState.VALID) {
			return null;
		}
		dict.updateLastAccessTime();
		return dict;
	}

	// During load, if dict is invalid, has to return it, and the load process will
	// check if the dict is up to date
	// If return null, it means the column is not related with a dict. Not need to send
	// the dict to be during load if it is null.
	public IDict getDictForLoad(long dbId, long tableId, String column) {
		IDict dict = this.dictsMap.get(new DictKey(dbId, tableId, column));
		if (dict == null) {
			return null;
		}
		return dict;
	}

	public void invalidDict(long dbId, long tableId, String column) {
		this.writeLock.lock();
		try {
			IDict dict = dictsMap.get(new DictKey(dbId, tableId, column));
			if (dict != null) {
				dict.invalidDict(true);
			}
			LOG.info("dict on db={}, table={}, column={} invalided, due to new data load", dbId, tableId, column);
		} finally {
			this.writeLock.unlock();
		}
	}
	
	public void removeDict(long dbId, long tableId, String column) {
		this.writeLock.lock();
		try {
			dictsMap.remove(new DictKey(dbId, tableId, column));
		} finally {
			this.writeLock.unlock();
		}
	}

	@Override
	protected void runOneCycle() {
		if (!Catalog.getCurrentCatalog().canRead()) {
			return;
		}
		// Traverse all dict, if the dict is not accessed for a long period of time,
		// then delete it
		// If the related database or column is dropped, then dict will be cleared in this code.
		long curTime = System.currentTimeMillis() / 1000;
		try {
			this.writeLock.lock();
			List<DictKey> dictToRemove = Lists.newArrayList();
			for (IDict dict : dictsMap.values()) {
				if (dict.getLastAccessTime() - curTime > Config.dict_expire_sec) {
					dictToRemove.add(dict.getDictKey());
				}
			}
			for (DictKey key : dictToRemove) {
				dictsMap.remove(key);
			}
		} finally {
			this.writeLock.unlock();
		}

		// Traverse all columns from catalog and check if it is needed to generate
		// global dict
		// This is a temporary work, because doris does not have a wonderful optimizer,
		// so that
		// check all column here instead.
		// Maybe this method could be moved to catalog class
		Catalog catalog = Catalog.getCurrentCatalog();
		List<Long> dbIds = catalog.getDbIds();
		for (Long dbId : dbIds) {
			Database db = catalog.getDbNullable(dbId);
			if (db == null) {
				continue;
			}
			List<Table> tables = db.getTables();
			for (Table table : tables) {
				if (table instanceof OlapTable) {
					OlapTable olapTable = (OlapTable) table;
					List<Column> allColumns = olapTable.getFullSchema();
					for (Column column : allColumns) {
						LOG.info("2dict key is {}", column.toSql());
						if (column.isLowCardinality()) {
							if (column.getDataType() == PrimitiveType.VARCHAR || column.getDataType() == PrimitiveType.CHAR
									|| column.getDataType() == PrimitiveType.STRING) {
								DictKey dictKey = new DictKey(db.getId(), olapTable.getId(), column.getName());
								LOG.info("dict key is {}", dictKey);
								if (!dictsMap.containsKey(dictKey)) {
									StringDict stringDict = new StringDict(this.dictIdGenerator.incrementAndGet(), db.getId(),
											olapTable.getId(), column.getName());
									// Has to set the empty dict invalid, for example,
									//   1. if fe set the column to low cardinality
									// 	 2. start a stream load, then the dictsmap does not contain the dict, so that it return null.
									//   3. fe will think the column not related with dict or not low cardinality, so that it will not
									// 		send dict to be, and be will not check the dict diff.
									//	 4. be commit txn and not set the column to invalid.
									//   5. fe could not find the dict is invalid, and maybe use it for future query, query failed.
									stringDict.invalidDict(true);
									dictsMap.put(dictKey, stringDict);
								} else {
									// Set dict num element info to column meta, it is just user for user to check the dict num
									// during show create table.
									column.setDictEleNum(dictsMap.get(dictKey).getDictElemNum());
								}
							}
						}
					}
				}
			}
		}
		// Call select dict(col) from table[meta] to get dict
		List<IDict> dictToRefresh = Lists.newArrayList();
		try {
			this.readLock.lock();
			for (IDict dict : dictsMap.values()) {
				if (dict.getState() == DictState.INVALID) {
					dictToRefresh.add(dict);
				}
			}
		} finally {
			this.readLock.unlock();
		}
		for (IDict dict : dictToRefresh) {
			IDict newDict = dict.refresh();
			if (newDict == null) {
				// Some errors happen during query, skip update the dict.
				continue;
			}
			if (newDict.dataChanged(dict)) {
				newDict.copyDictState(dict);
				newDict.invalidDict(true);
			}
			// If cur data version is 10, then use version = 10 to refresh dict, but current
			// load is using old
			// dict, then it will increase data version to a larger value for example 15.
			// After refresh finished, then
			// the dict will find that version 10 < version 15, then it will invalid the
			// dict. If we do not replace the
			// old dict, load process will increase data version to more larger value and we
			// could not catchup. But if we
			// replace the old dict with dict (version = 10), then the new key maybe covered
			// by the new dict, load process
			// will use the new dict to encode data, it will find there is no new key. Then
			// the data version will kept to be
			// 15. Then in next round dict will refresh to 15. And it is valid, could be
			// used during query.
			// replace the old dict with new dict, then load process will use the newly dict
			// if the load process does not find any new key, then the it will not call
			// invalid
			newDict.resetDictId(this.dictIdGenerator.incrementAndGet());
			LOG.info("get new dict {}", newDict.toString());
			try {
				this.writeLock.lock();
				dictsMap.put(dict.getDictKey(), newDict);
			} finally {
				this.writeLock.unlock();
			}
		}
	}
}
