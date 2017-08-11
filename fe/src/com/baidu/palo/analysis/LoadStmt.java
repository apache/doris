// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.util.PrintableMap;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

// LOAD statement, load files into tables.
//
// syntax:
//      LOAD LABEL load_label
//          (data_desc, ...)
//          [BY cluster]
//          [PROPERTIES (key1=value1, )]
//
//      load_label:
//          db_name.label_name
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator ]
//          [(col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xx))]
public class LoadStmt extends DdlStmt {
    public static final String TIMEOUT_PROPERTY = "timeout";
    public static final String MAX_FILTER_RATIO_PROPERTY = "max_filter_ratio";
    public static final String LOAD_DELETE_FLAG_PROPERTY = "load_delete_flag";
    public static final String CLUSTER_PROPERTY = "cluster";

    // for load data from Baidu Object Store(BOS)
    public static final String BOS_ENDPOINT = "bos_endpoint";
    public static final String BOS_ACCESSKEY = "bos_accesskey";
    public static final String BOS_SECRET_ACCESSKEY = "bos_secret_accesskey";

    // mini load params
    public static final String KEY_IN_PARAM_COLUMNS = "columns";
    public static final String KEY_IN_PARAM_SET= "set";
    public static final String KEY_IN_PARAM_HLL= "hll";
    public static final String KEY_IN_PARAM_COLUMN_SEPARATOR = "column_separator";
    public static final String KEY_IN_PARAM_LINE_DELIMITER = "line_delimiter";
    public static final String KEY_IN_PARAM_PARTITIONS = "partitions";

    private final LabelName label;
    private final List<DataDescription> dataDescriptions;
    private final BrokerDesc brokerDesc;
    private final String cluster;
    private final Map<String, String> properties;
    private String user;

    public LoadStmt(LabelName label, List<DataDescription> dataDescriptions,
                    BrokerDesc brokerDesc, String cluster, Map<String, String> properties) {
        this.label = label;
        this.dataDescriptions = dataDescriptions;
        this.brokerDesc = brokerDesc;
        this.cluster = cluster;
        this.properties = properties;
        this.user = null;
    }

    public LabelName getLabel() {
        return label;
    }

    public List<DataDescription> getDataDescriptions() {
        return dataDescriptions;
    }

    public BrokerDesc getBrokerDesc() {
        return brokerDesc;
    }

    public String getCluster() {
        return cluster;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getUser() {
        return user;
    }

    public static void checkProperties(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            return;
        }

        Set<String> propertySet = Sets.newHashSet();
        propertySet.add(LoadStmt.TIMEOUT_PROPERTY);
        propertySet.add(LoadStmt.MAX_FILTER_RATIO_PROPERTY);
        propertySet.add(LoadStmt.LOAD_DELETE_FLAG_PROPERTY);

        for (Entry<String, String> entry : properties.entrySet()) {
            if (!propertySet.contains(entry.getKey())) {
                throw new DdlException(entry.getKey() + "is invalid property");
            }
        }

        if (properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY) != null) {
            double maxFilterRatio = 0.0;
            try {
                maxFilterRatio = Double.valueOf(properties.get(LoadStmt.MAX_FILTER_RATIO_PROPERTY));
            } catch (NumberFormatException e) {
                throw new DdlException(LoadStmt.MAX_FILTER_RATIO_PROPERTY + " is not a number.");

            }
            if (maxFilterRatio < 0.0 || maxFilterRatio > 1.0) {
                throw new DdlException(LoadStmt.MAX_FILTER_RATIO_PROPERTY + " must between 0.0 and 1.0.");
            }
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        label.analyze(analyzer);
        if (dataDescriptions == null || dataDescriptions.isEmpty()) {
            throw new AnalysisException("No data file in load statement.");
        }
        for (DataDescription dataDescription : dataDescriptions) {
            if (brokerDesc != null) {
                dataDescription.setIsPullLoad(true);
            }
            dataDescription.analyze();
        }
        
        // check auth
        user = analyzer.getUser();
        if (!analyzer.getCatalog().getUserMgr().checkAccess(user, label.getDbName(), AccessPrivilege.READ_WRITE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, user, label.getDbName());
        }

        try {
            checkProperties(properties);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD LABEL ").append(label.toSql()).append("\n");
        sb.append("(");
        Joiner.on(",\n").appendTo(sb, Lists.transform(dataDescriptions, new Function<DataDescription, Object>() {
            @Override
            public Object apply(DataDescription dataDescription) {
                return dataDescription.toSql();
            }
        })).append(")");
        if (cluster != null) {
            sb.append("\nBY '");
            sb.append(cluster);
            sb.append("'");
        }
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
