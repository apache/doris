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

package org.apache.doris.policy;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * DropPolicyLog
 *
 * @author lirongqian
 * @since 2022/04/24
 */
@AllArgsConstructor
@Getter
public class DropPolicyLog implements Writable {
    
    @SerializedName(value = "dbId")
    private long dbId;
    
    @SerializedName(value = "tableId")
    private long tableId;
    
    @SerializedName(value = "type")
    private String type;
    
    @SerializedName(value = "policyName")
    private String policyName;
    
    @SneakyThrows
    public static DropPolicyLog fromDropStmt(DropPolicyStmt stmt) {
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(stmt.getTableName().getDb());
        Table table = db.getTableOrAnalysisException(stmt.getTableName().getTbl());
        return new DropPolicyLog(db.getId(), table.getId(), stmt.getType(), stmt.getPolicyName());
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }
    
    public static DropPolicyLog read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), DropPolicyLog.class);
    }
}
