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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;

import com.google.common.collect.Lists;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class ExternalFileTableValuedFunction extends TableValuedFunctionIf {
    public static final Logger LOG = LogManager.getLogger(ExternalFileTableValuedFunction.class);

    public abstract PFetchTableSchemaRequest getFetchTableStructureRequest();


    // TODO(ftw): no use, deleted.
    public enum FileFormatType {
        CSV, CSV_WITH_NAMES, CSV_WITH_NAMES_AND_TYPES;
        public String toFileFormatName() {
            switch (this) {
                case CSV:
                    return "csv";
                // case CSV_WITH_NAMES:
                //     return "csv_with_names";
                // case CSV_WITH_NAMES_AND_TYPES:
                //     return "csv_with_names_and_types";
                default:
                    return null;
            }
        }
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        // get one BE address
        TNetworkAddress address = null;
        List<Column> columns = null;
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
                break;
            }
        }
        if (address == null) {
            throw new AnalysisException("No Alive backends");
        }
        PFetchTableSchemaRequest request = getFetchTableStructureRequest();
        try {
            Future<InternalService.PFetchTableSchemaResult> future = BackendServiceProxy.getInstance()
                    .fetchTableStructureAsync(address, request);
            InternalService.PFetchTableSchemaResult result = future.get();
            columns = Lists.newArrayList();
            fillColumns(result, columns);
        } catch (RpcException e) {
            throw new AnalysisException("fetchTableStructureResult rpc exception", e);
        } catch (InterruptedException e) {
            throw new AnalysisException("fetchTableStructureResult interrupted exception", e);
        } catch (ExecutionException e) {
            throw new AnalysisException("fetchTableStructureResult exception", e);
        }
        return columns;
    }

    @Override
    public List<TableValuedFunctionTask> getTasks() throws AnalysisException {
        return null;
    }

    private void fillColumns(InternalService.PFetchTableSchemaResult result, List<Column> columns)
                            throws AnalysisException {
        if (result.getColumnNums() == 0) {
            throw new AnalysisException("The amount of column is 0");
        }
        for (int idx = 0; idx < result.getColumnNums(); ++idx) {
            PTypeDesc type = result.getColumnTypes(idx);
            String colName = result.getColumnNames(idx);
            for (PTypeNode typeNode : type.getTypesList()) {
                // only support ScalarType.
                PScalarType scalarType = typeNode.getScalarType();
                TPrimitiveType tPrimitiveType = TPrimitiveType.findByValue(scalarType.getType());
                columns.add(new Column(colName, PrimitiveType.fromThrift(tPrimitiveType), true));
            }
        }
    }
}
