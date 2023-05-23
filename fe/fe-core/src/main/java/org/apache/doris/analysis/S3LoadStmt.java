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

import org.apache.doris.analysis.StorageBackend.StorageType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.constants.S3Properties.Env;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * For TVF solution validation.
 */
public class S3LoadStmt extends NativeInsertStmt {

    public S3LoadStmt(LabelName label, List<DataDescription> dataDescList, BrokerDesc brokerDesc,
            Map<String, String> properties, String comments) throws DdlException {
        super(buildInsertTarget(dataDescList.get(0)),
                label.getLabelName(), /*TODO(tsy): think about columns*/null,
                buildInsertSource(dataDescList.get(0), brokerDesc), null);
    }

    private static InsertTarget buildInsertTarget(DataDescription dataDescription) {
        final TableName tableName = new TableName(null, null, dataDescription.getTableName());
        return new InsertTarget(tableName, dataDescription.getPartitionNames());
    }

    private static InsertSource buildInsertSource(DataDescription dataDescription, BrokerDesc brokerDesc)
            throws DdlException {
        final SelectList selectList = new SelectList();
        // TODO(tsy): think about columns, now use `select *` way
        final SelectListItem item = new SelectListItem(SelectListItem.createStarItem(null));
        selectList.addItem(item);

        final FromClause fromClause = new FromClause(
                Collections.singletonList(buildTvfRef(dataDescription, brokerDesc))
        );
        final TableName tableName = new TableName(null, null, dataDescription.getTableName());
        final OrderByElement orderByElement = new OrderByElement(
                new SlotRef(tableName, dataDescription.getSequenceCol()),
                true, null
        );

        final SelectStmt selectStmt = new SelectStmt(
                selectList, fromClause, /*TODO(tsy): think about PRECEDING FILTER*/dataDescription.getWhereExpr(),
                null, null,
                Lists.newArrayList(orderByElement), LimitElement.NO_LIMIT
        );
        return new InsertSource(selectStmt);
    }

    private static TableRef buildTvfRef(DataDescription dataDescription, BrokerDesc brokerDesc) throws DdlException {
        final Map<String, String> params = Maps.newHashMap();

        final List<String> filePaths = dataDescription.getFilePaths();
        Preconditions.checkState(filePaths.size() == 1, "there should be only one file path");
        final String s3FilePath = filePaths.get(0);
        params.put(S3TableValuedFunction.S3_URI, s3FilePath);

        final Map<String, String> dataDescProp = dataDescription.getProperties();
        if (dataDescProp != null) {
            params.putAll(dataDescProp);
        }

        params.put(ExternalFileTableValuedFunction.FORMAT, dataDescription.getFileFormat());
        params.put(ExternalFileTableValuedFunction.COLUMN_SEPARATOR, dataDescription.getColumnSeparator());

        Preconditions.checkState(!brokerDesc.isMultiLoadBroker(), "do not support multi broker load currently");
        Preconditions.checkState(brokerDesc.getStorageType() == StorageType.S3, "only support S3 load");

        final Map<String, String> s3ResourceProp = brokerDesc.getProperties();
        S3Properties.convertToStdProperties(s3ResourceProp);
        s3ResourceProp.keySet().removeIf(Env.FS_KEYS::contains);
        params.putAll(s3ResourceProp);

        try {
            return new TableValuedFunctionRef(S3TableValuedFunction.NAME, null, params);
        } catch (AnalysisException e) {
            throw new DdlException("failed to create s3 tvf ref", e);
        }
    }
}
