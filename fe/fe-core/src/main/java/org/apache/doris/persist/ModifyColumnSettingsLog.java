//Licensed to the Apache Software Foundation (ASF) under one
//or more contributor license agreements.  See the NOTICE file
//distributed with this work for additional information
//regarding copyright ownership.  The ASF licenses this file
//to you under the Apache License, Version 2.0 (the
//"License"); you may not use this file except in compliance
//with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing,
//software distributed under the License is distributed on an
//"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//KIND, either express or implied.  See the License for the
//specific language governing permissions and limitations
//under the License.

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.alibaba.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

//Persist the info when removing batch of expired txns
public class ModifyColumnSettingsLog implements Writable {

	@SerializedName(value = "dbId")
	private long dbId;
	@SerializedName(value = "tblId")
	private long tblId;
	@SerializedName(value = "lowCardinalitySettings")
	// ColumnName-->LowCardinalitySettings
	private Map<String, Boolean> lowCardinalitySettings;

	public ModifyColumnSettingsLog(long dbId, long tblId) {
		this.dbId = dbId;
		this.tblId = tblId;
		this.lowCardinalitySettings = Maps.newHashMap();
	}

	public long getDbId() {
		return dbId;
	}

	public long getTblId() {
		return tblId;
	}

	public void setLowCardinality(String colName, boolean lowCardinality) {
		lowCardinalitySettings.put(colName, lowCardinality);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		String json = GsonUtils.GSON.toJson(this);
		Text.writeString(out, json);
	}

	public static ModifyColumnSettingsLog read(DataInput in) throws IOException {
		String json = Text.readString(in);
		return GsonUtils.GSON.fromJson(json, ModifyColumnSettingsLog.class);
	}

	public Map<String, Boolean> getLowCardinalitySettings() {
		return lowCardinalitySettings;
	}
}
