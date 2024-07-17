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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

@Data
public class DorisDataMaskPolicy extends Policy implements DataMaskPolicy {

    public static final ShowResultSetMetaData DATA_MASK_META_DATA =
            ShowResultSetMetaData.builder()
            .addColumn(new Column("PolicyName", ScalarType.createVarchar(100)))
            .addColumn(new Column("CatalogName", ScalarType.createVarchar(100)))
            .addColumn(new Column("DbName", ScalarType.createVarchar(100)))
            .addColumn(new Column("TableName", ScalarType.createVarchar(100)))
            .addColumn(new Column("ColumnName", ScalarType.createVarchar(100)))
            .addColumn(new Column("DataMaskType", ScalarType.createVarchar(20)))
            .addColumn(new Column("DataMaskDef", ScalarType.createVarchar(65533)))
            .addColumn(new Column("User", ScalarType.createVarchar(200)))
            .addColumn(new Column("Role", ScalarType.createVarchar(200))).build();

    private static final Logger LOG = LogManager.getLogger(DorisDataMaskPolicy.class);

    /**
     * Policy bind user.
     **/
    @SerializedName(value = "user")
    private UserIdentity user;

    @SerializedName(value = "roleName")
    private String roleName;

    @SerializedName(value = "ctlName")
    private String ctlName;

    @SerializedName(value = "dbName")
    private String dbName;

    @SerializedName(value = "tableName")
    private String tableName;

    @SerializedName(value = "colName")
    private String colName;

    @SerializedName(value = "maskType")
    private DataMaskType maskType;

    public DorisDataMaskPolicy(long id, String policyName, UserIdentity user, String roleName, String ctlName,
                               String dbName, String tableName, String colName, String maskType)
            throws AnalysisException {
        super(id, PolicyTypeEnum.DATA_MASK, policyName);
        Objects.requireNonNull(maskType, "require maskType object");
        this.user = user;
        this.roleName = roleName;
        this.ctlName = ctlName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.colName = colName;
        this.maskType = find(maskType.toUpperCase(Locale.ROOT));
    }

    public DorisDataMaskPolicy(UserIdentity user, String roleName) {
        this.type = PolicyTypeEnum.DATA_MASK;
        this.user = user;
        this.roleName = roleName;
    }

    private DataMaskType find(String typeDef) throws AnalysisException {
        try {
            return DataMaskType.valueOf(typeDef);
        } catch (Exception e) {
            throw new AnalysisException("no such data mask type : " + typeDef);
        }
    }

    @Override
    public String getMaskTypeDef() {
        if (maskType == DataMaskType.MASK_NULL) {
            return maskType.getTransformer();
        }
        return maskType.getTransformer().replace("{col}", colName);
    }

    @Override
    public String getPolicyIdent() {
        return null;
    }

    @Override
    public Expression parseMaskTypeDef(NereidsParser parser, Slot slot) {
        if (maskType == DataMaskType.MASK_DEFAULT) {
            return StringLiteral.of(getDataTypeDefaultValue(slot));
        }
        return parser.parseExpression(getMaskTypeDef());
    }

    @Override
    public void gsonPostProcess() throws IOException {

    }

    @Override
    public List<String> getShowInfo() throws AnalysisException {
        return Lists.newArrayList(this.policyName, ctlName, dbName, tableName, colName,
            this.maskType.name(), this.maskType.getTransformer(),
            this.user == null ? null : this.user.getQualifiedUser(), this.roleName);
    }

    @Override
    public boolean isInvalid() {
        return false;
    }

    @Override
    public boolean matchPolicy(Policy checkedPolicyCondition) {
        DorisDataMaskPolicy maskPolicy = (DorisDataMaskPolicy) checkedPolicyCondition;
        return checkMatched(maskPolicy.getCtlName(), maskPolicy.getDbName(), maskPolicy.getTableName(),
            maskPolicy.getColName(), maskPolicy.getType(), maskPolicy.getPolicyName(), maskPolicy.getUser(),
            maskPolicy.getRoleName(), maskPolicy.getMaskType());
    }

    private boolean checkMatched(String ctlName, String dbName, String tableName, String colName, PolicyTypeEnum type,
                                 String policyName, UserIdentity user, String roleName, DataMaskType maskType) {
        if (policyName != null && StringUtils.equals(policyName, this.policyName)) {
            return true;
        }
        return type.equals(this.type)
            && (StringUtils.isEmpty(ctlName) || StringUtils.equals(ctlName, this.ctlName))
            && (StringUtils.isEmpty(dbName) || StringUtils.equals(dbName, this.dbName))
            && (StringUtils.isEmpty(tableName) || StringUtils.equals(tableName, this.tableName))
            && (StringUtils.isEmpty(colName) || StringUtils.equals(colName, this.colName))
            && (StringUtils.isEmpty(roleName) || StringUtils.equals(roleName, this.roleName))
            && (user == null || Objects.equals(user, this.user))
            && (maskType == null || Objects.equals(maskType, this.maskType));
    }

    public String getDataTypeDefaultValue(Slot slot) {
        Type dataType = slot.getDataType().toCatalogDataType();
        if (dataType instanceof ScalarType) {
            switch (dataType.getPrimitiveType()) {
                case BOOLEAN:
                    return "FALSE";
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                case LARGEINT:
                    return "0";
                case CHAR:
                case VARCHAR:
                case STRING:
                    return "";
                case FLOAT:
                case DOUBLE:
                case DECIMALV2:
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                case DECIMAL256:
                    return "0.0";
                case DATEV2:
                case DATE:
                    return "1970-01-01";
                case TIME:
                case TIMEV2:
                    return "00:00:00";
                case DATETIME:
                case DATETIMEV2:
                    return "1970-01-01 00:00:00";
                case IPV4:
                    return "0.0.0.0";
                case ARRAY:
                    return "[]";
                case MAP:
                case JSONB:
                case STRUCT:
                    return "{}";
                default:
                    return "NULL";
            }
        }
        return "NULL";
    }
}
