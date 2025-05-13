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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TDorisTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Setter
@Getter
public class DorisTable extends Table {
    private static final Logger LOG = LogManager.getLogger(DorisTable.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String FE_NODES = "fe_nodes";
    private static final String FE_ARROW_NODES = "fe_arrow_nodes";
    private static final String BE_NODES = "be_nodes";

    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String TABLE = "table";
    private static final String MAX_EXEC_BE_NUM = "max_exec_be_num";
    private static final String HTTP_SSL_ENABLED = "http_ssl_enabled";

    private List<String> feNodes = new ArrayList<>();
    private List<String> feArrowNodes = new ArrayList<>();
    private List<String> beNodes = new ArrayList<>();

    private String userName = "";
    private String password = "";
    private String externalTableName;

    private int maxExecBeNum = Integer.parseInt(DorisResource.MAX_EXEC_BE_NUM_DEFAULT_VALUE);
    private boolean httpSslEnabled = Boolean.parseBoolean(DorisResource.HTTP_SSL_ENABLED_DEFAULT_VALUE);

    public DorisTable(long id, String name, List<Column> schema, Map<String, String> properties)
            throws DdlException {
        super(id, name, TableType.DORIS, schema);
        validate(properties);
    }

    public DorisTable(long id, String name, List<Column> schema, TableType type) {
        super(id, name, type, schema);
    }

    public DorisTable() {
        super(TableType.DORIS);
    }

    @Override
    public TTableDescriptor toThrift() {
        TDorisTable tDorisTable = new TDorisTable();
        tDorisTable.setFeNodes(feNodes);
        tDorisTable.setFeArrowNodes(feArrowNodes);
        tDorisTable.setUserName(userName);
        tDorisTable.setPasswd(password);
        tDorisTable.setTableName(externalTableName);

        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.DORIS_TABLE, fullSchema.size(), 0,
                getName(), "");

        tTableDescriptor.setDorisTable(tDorisTable);
        tTableDescriptor.setTableName(externalTableName);
        return tTableDescriptor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Map<String, String> serializeMap = Maps.newHashMap();
        serializeMap.put(FE_NODES, objectMapper.writeValueAsString(feNodes));
        serializeMap.put(FE_ARROW_NODES, objectMapper.writeValueAsString(feArrowNodes));
        serializeMap.put(USER, userName);
        serializeMap.put(PASSWORD, password);
        serializeMap.put(MAX_EXEC_BE_NUM, String.valueOf(maxExecBeNum));
        serializeMap.put(HTTP_SSL_ENABLED, String.valueOf(httpSslEnabled));
        serializeMap.put(TABLE, externalTableName);
        serializeMap.put(BE_NODES, objectMapper.writeValueAsString(beNodes));

        int size = (int) serializeMap.values().stream().filter(v -> {
            return v != null;
        }).count();
        out.writeInt(size);

        for (Map.Entry<String, String> kv : serializeMap.entrySet()) {
            if (kv.getValue() != null) {
                Text.writeString(out, kv.getKey());
                Text.writeString(out, kv.getValue());
            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        int size = in.readInt();
        Map<String, String> serializeMap = Maps.newHashMap();
        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            serializeMap.put(key, value);
        }

        String feNodesJson = serializeMap.get(FE_NODES);
        if (feNodesJson != null) {
            feNodes = objectMapper.readValue(feNodesJson, new TypeReference<List<String>>() {});
        } else {
            feNodes = Lists.newArrayList();
        }

        String feArrowNodesJson = serializeMap.get(FE_ARROW_NODES);
        if (feArrowNodesJson != null) {
            feArrowNodes = objectMapper.readValue(feArrowNodesJson, new TypeReference<List<String>>() {});
        } else {
            feArrowNodes = Lists.newArrayList();
        }

        String beNodesJson = serializeMap.get(BE_NODES);
        if (beNodesJson != null) {
            beNodes = objectMapper.readValue(beNodesJson, new TypeReference<List<String>>() {});
        } else {
            beNodes = Lists.newArrayList();
        }

        userName = serializeMap.get(USER);
        password = serializeMap.get(PASSWORD);
        maxExecBeNum = serializeMap.get(MAX_EXEC_BE_NUM) != null
            ? Integer.parseInt(serializeMap.get(MAX_EXEC_BE_NUM))
            : Integer.parseInt(DorisResource.MAX_EXEC_BE_NUM_DEFAULT_VALUE);
        httpSslEnabled = serializeMap.get(HTTP_SSL_ENABLED) != null
            ? Boolean.parseBoolean(serializeMap.get(HTTP_SSL_ENABLED))
            : Boolean.parseBoolean(DorisResource.HTTP_SSL_ENABLED_DEFAULT_VALUE);
        externalTableName = serializeMap.get(TABLE);
    }

    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type.name());
        sb.append(externalTableName);
        sb.append(String.join(",", feNodes));
        sb.append(String.join(",", feArrowNodes));
        sb.append(String.join(",", beNodes));
        sb.append(userName);
        sb.append(password);

        String md5 = DigestUtils.md5Hex(sb.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of odbc table {}: {}. signature string: {}", name, md5, sb.toString());
        }
        return md5;
    }

    @Override
    public DorisTable clone() {
        DorisTable copied = new DorisTable();
        if (!DeepCopy.copy(this, copied, DorisTable.class, FeConstants.meta_version)) {
            LOG.warn("failed to copy doris table: " + getName());
            return null;
        }
        return copied;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of doris table, "
                + "they are: fe host, fe arrow host, user, password, database and table");
        }

        externalTableName = properties.get(TABLE);
        if (Strings.isNullOrEmpty(externalTableName)) {
            throw new DdlException("property " + TABLE + " must be set");
        }

        password = properties.get(PASSWORD);
        if (Strings.isNullOrEmpty(password)) {
            throw new DdlException("property " + PASSWORD + " must be set");
        }

        userName = properties.get(USER);
        if (Strings.isNullOrEmpty(userName)) {
            throw new DdlException("property " + USER + " must be set");
        }

        String feNodesStr = properties.get(FE_NODES);
        if (Strings.isNullOrEmpty(feNodesStr)) {
            throw new DdlException("property " + FE_NODES + " must be set");
        }
        feNodes = Arrays.asList(feNodesStr.trim().split(","));

        String feArrowNodesStr = properties.get(FE_ARROW_NODES);
        if (Strings.isNullOrEmpty(feArrowNodesStr)) {
            throw new DdlException("property " + FE_ARROW_NODES + " must be set");
        }
        feArrowNodes = Arrays.asList(feArrowNodesStr.trim().split(","));

        if (Strings.isNullOrEmpty(properties.get(BE_NODES))) {
            beNodes = Arrays.asList(properties.get(BE_NODES).trim().split(","));
        }

        if (Strings.isNullOrEmpty(properties.get(MAX_EXEC_BE_NUM))) {
            maxExecBeNum = Integer.parseInt(properties.get(MAX_EXEC_BE_NUM));
        }

        externalTableName = properties.get(HTTP_SSL_ENABLED);
    }
}
