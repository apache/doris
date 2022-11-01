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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.collect.Lists;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class ExternalFileTableValuedFunction extends TableValuedFunctionIf {
    public static final Logger LOG = LogManager.getLogger(ExternalFileTableValuedFunction.class);

    protected List<Column> columns = null;
    protected List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();

    public abstract PFetchTableSchemaRequest getFetchTableStructureRequest() throws AnalysisException, TException;

    public abstract TFileFormatType getTFileFormatType();

    public abstract TFileType getTFileType();

    public abstract Map<String, String> getLocationProperties();

    public abstract String getColumnSeparator();

    public abstract String getLineSeparator();

    public abstract String getFilePath();

    public abstract BrokerDesc getBrokerDesc();

    public abstract String getHeaderType();

    public void parseFile() throws UserException {
        String path = getFilePath();
        BrokerDesc brokerDesc = getBrokerDesc();
        BrokerUtil.parseFile(path, brokerDesc, fileStatuses);
    }

    public List<TBrokerFileStatus> getFileStatuses() {
        return fileStatuses;
    }

    @Override
    public List<Column> getTableColumns() throws AnalysisException {
        if (this.columns != null) {
            return columns;
        }
        // get one BE address
        TNetworkAddress address = null;
        columns = Lists.newArrayList();
        for (Backend be : Env.getCurrentSystemInfo().getIdToBackend().values()) {
            if (be.isAlive()) {
                address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
                break;
            }
        }
        if (address == null) {
            throw new AnalysisException("No Alive backends");
        }

        try {
            PFetchTableSchemaRequest request = getFetchTableStructureRequest();
            Future<InternalService.PFetchTableSchemaResult> future = BackendServiceProxy.getInstance()
                    .fetchTableStructureAsync(address, request);

            InternalService.PFetchTableSchemaResult result = future.get();
            TStatusCode code = TStatusCode.findByValue(result.getStatus().getStatusCode());
            String errMsg;
            if (code != TStatusCode.OK) {
                if (!result.getStatus().getErrorMsgsList().isEmpty()) {
                    errMsg = result.getStatus().getErrorMsgsList().get(0);
                } else {
                    errMsg =  "fetchTableStructureAsync failed. backend address: "
                                + address.getHostname() + ":" + address.getPort();
                }
                throw new AnalysisException(errMsg);
            }

            fillColumns(result);
        } catch (RpcException e) {
            throw new AnalysisException("fetchTableStructureResult rpc exception", e);
        } catch (InterruptedException e) {
            throw new AnalysisException("fetchTableStructureResult interrupted exception", e);
        } catch (ExecutionException e) {
            throw new AnalysisException("fetchTableStructureResult exception", e);
        } catch (TException e) {
            throw new AnalysisException("getFetchTableStructureRequest exception", e);
        }
        return columns;
    }

    private void fillColumns(InternalService.PFetchTableSchemaResult result)
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
