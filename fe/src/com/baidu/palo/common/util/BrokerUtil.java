package com.baidu.palo.common.util;

import com.baidu.palo.analysis.BrokerDesc;
import com.baidu.palo.catalog.BrokerMgr;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.service.FrontendOptions;
import com.baidu.palo.thrift.TBrokerFileStatus;
import com.baidu.palo.thrift.TBrokerListPathRequest;
import com.baidu.palo.thrift.TBrokerListResponse;
import com.baidu.palo.thrift.TBrokerOperationStatusCode;
import com.baidu.palo.thrift.TBrokerVersion;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    public static void parseBrokerFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws InternalException {
        BrokerMgr.BrokerAddress brokerAddress = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            brokerAddress = Catalog.getInstance().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        } catch (AnalysisException e) {
            throw new InternalException(e.getMessage());
        }
        TNetworkAddress address = new TNetworkAddress(brokerAddress.ip, brokerAddress.port);
        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            try {
                client = ClientPool.brokerPool.borrowObject(address);
            } catch (Exception e1) {
                throw new InternalException("Create connection to broker(" + address + ") failed.");
            }
        }
        boolean failed = true;
        try {
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = null;
            try {
                tBrokerListResponse = client.listPath(request);
            } catch (TException e) {
                ClientPool.brokerPool.reopen(client);
                tBrokerListResponse = client.listPath(request);
            }
            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new InternalException("Broker list path failed.path=" + path
                        + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            failed = false;
            for (TBrokerFileStatus tBrokerFileStatus : tBrokerListResponse.getFiles()) {
                if (tBrokerFileStatus.isDir) {
                    continue;
                }
                fileStatuses.add(tBrokerFileStatus);
            }
        } catch (TException e) {
            LOG.warn("Broker list path exception, path={}, address={}, exception={}", path, address, e);
            throw new InternalException("Broker list path exception.path=" + path + ",broker=" + address);
        } finally {
            if (failed) {
                ClientPool.brokerPool.invalidateObject(address, client);
            } else {
                ClientPool.brokerPool.returnObject(address, client);
            }
        }
    }

}
