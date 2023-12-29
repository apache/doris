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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

public class BrokerLoadStmt extends InsertStmt {

    private final List<DataDescription> dataDescList;

    private final BrokerDesc brokerDesc;

    private String cluster;

    public BrokerLoadStmt(LabelName label, List<DataDescription> dataDescList, BrokerDesc brokerDesc,
            Map<String, String> properties, String comments) {
        super(label, properties, comments);
        this.dataDescList = dataDescList;
        this.brokerDesc = brokerDesc;
    }

    @Override
    public List<DataDescription> getDataDescList() {
        return dataDescList;
    }

    @Override
    public BrokerDesc getResourceDesc() {
        return brokerDesc;
    }

    @Override
    public LoadType getLoadType() {
        return LoadType.BROKER_LOAD;
    }

    @Override
    public void analyzeProperties() throws DdlException {
        // public check should be in base class
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        label.analyze(analyzer);
        Preconditions.checkState(!CollectionUtils.isEmpty(dataDescList),
                new AnalysisException("No data file in load statement."));
        Preconditions.checkNotNull(brokerDesc, "No broker desc found.");
        // check data descriptions
        for (DataDescription dataDescription : dataDescList) {
            final String fullDbName = dataDescription.analyzeFullDbName(label.getDbName(), analyzer);
            dataDescription.analyze(fullDbName);
            Preconditions.checkState(!dataDescription.isLoadFromTable(),
                    new AnalysisException("Load from table should use Spark Load"));
            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(fullDbName);
            OlapTable table = db.getOlapTableOrAnalysisException(dataDescription.getTableName());
            dataDescription.checkKeyTypeForLoad(table);
            if (!brokerDesc.isMultiLoadBroker()) {
                for (int i = 0; i < dataDescription.getFilePaths().size(); i++) {
                    String location = brokerDesc.getFileLocation(dataDescription.getFilePaths().get(i));
                    dataDescription.getFilePaths().set(i, location);
                    StorageBackend.checkPath(dataDescription.getFilePaths().get(i),
                            brokerDesc.getStorageType(), "DATA INFILE must be specified.");
                    dataDescription.getFilePaths().set(i, dataDescription.getFilePaths().get(i));
                }
            }
        }
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD LABEL ").append(label.toSql()).append("\n");
        sb.append("(");
        Joiner.on(",\n").appendTo(sb, Lists.transform(dataDescList, DataDesc::toSql)).append(")");
        if (cluster != null) {
            sb.append("\nBY '");
            sb.append(cluster);
            sb.append("'");
        }
        if (brokerDesc != null) {
            sb.append("\n").append(brokerDesc.toSql());
        }

        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<>(properties, "=", true, false, true));
            sb.append(")");
        }
        return sb.toString();
    }
}
