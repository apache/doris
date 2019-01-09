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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.external.EsTableState;
import org.apache.doris.thrift.TEsTable;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.doris.thrift.TTableType;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

public class EsTable extends Table {
    private static final Logger LOG = LogManager.getLogger(EsTable.class);

    public static final String HOSTS = "hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String INDEX = "index";
    public static final String TYPE = "type";

    private String hosts;
    private String[] seeds;
    private String userName;
    private String passwd;
    private String indexName;
    private String mappingType = "doc";
    // only save the partition definition, save the partition key,
    // partition list is got from es cluster dynamically and is saved in esTableState
    private PartitionInfo partitionInfo;
    private EsTableState esTableState;

    public EsTable() {
        super(TableType.ELASTICSEARCH);
    }

    public EsTable(long id, String name, List<Column> schema, 
            Map<String, String> properties, PartitionInfo partitionInfo)
            throws DdlException {
        super(id, name, TableType.ELASTICSEARCH, schema);
        this.partitionInfo = partitionInfo;
        validate(properties);
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of elasticsearch table, "
                    + "they are: hosts, user, password, index");
        }

        hosts = properties.get(HOSTS);
        if (Strings.isNullOrEmpty(hosts)) {
            throw new DdlException("Hosts of ES table is null. "
                    + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
        }
        seeds = hosts.split(",");
        // TODO(ygl) validate the seeds? 

        userName = properties.get(USER);
        if (Strings.isNullOrEmpty(userName)) {
            userName = "";
        }

        passwd = properties.get(PASSWORD);
        if (passwd == null) {
            passwd = "";
        }

        indexName = properties.get(INDEX);
        if (Strings.isNullOrEmpty(indexName)) {
            throw new DdlException("Index of ES table is null. "
                    + "Please add properties('index'='xxxx') when create table");
        }

        mappingType = properties.get(TYPE);
        if (Strings.isNullOrEmpty(mappingType)) {
            mappingType = "docs";
        }
    }
    
    public TTableDescriptor toThrift() {
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE,
                baseSchema.size(), 0, getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        String charsetName = "UTF-8";

        try {
            // name
            adler32.update(name.getBytes(charsetName));
            // type
            adler32.update(type.name().getBytes(charsetName));
            // host
            adler32.update(hosts.getBytes(charsetName));
            // username
            adler32.update(userName.getBytes(charsetName));
            // passwd
            adler32.update(passwd.getBytes(charsetName));
            // mysql db
            adler32.update(indexName.getBytes(charsetName));
            // mysql table
            adler32.update(mappingType.getBytes(charsetName));

        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, hosts);
        Text.writeString(out, userName);
        Text.writeString(out, passwd);
        Text.writeString(out, indexName);
        Text.writeString(out, mappingType);
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        hosts = Text.readString(in);
        seeds = hosts.split(",");
        userName = Text.readString(in);
        passwd = Text.readString(in);
        indexName = Text.readString(in);
        mappingType = Text.readString(in);
        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = SinglePartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }
    }

    public String getHosts() {
        return hosts;
    }
    
    public String[] getSeeds() {
        return seeds;
    }

    public String getUserName() {
        return userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public EsTableState getEsTableState() {
        return esTableState;
    }

    public void setEsTableState(EsTableState esTableState) {
        this.esTableState = esTableState;
    }
}
