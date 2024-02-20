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

package org.apache.doris.datasource.es;

import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents one node with the http and potential thrift publish address
 */
public class EsNodeInfo {
    private final String id;
    private final String name;
    private final String host;
    private final String ip;
    private TNetworkAddress publishAddress;
    private final boolean hasHttp;
    private final boolean isClient;
    private final boolean isData;
    private final boolean isIngest;
    private boolean hasThrift;
    private TNetworkAddress thriftAddress;

    private static final Logger LOG = LogManager.getLogger(EsNodeInfo.class);

    public EsNodeInfo(String id, Map<String, Object> map, boolean httpSslEnabled) {
        this.id = id;
        EsMajorVersion version = EsMajorVersion.parse((String) map.get("version"));
        this.name = (String) map.get("name");
        this.host = (String) map.get("host");
        this.ip = (String) map.get("ip");
        if (version.before(EsMajorVersion.V_5_X)) {
            Map<String, Object> attributes = (Map<String, Object>) map.get("attributes");
            if (attributes == null) {
                this.isClient = false;
                this.isData = true;
            } else {
                String data = (String) attributes.get("data");
                this.isClient = data == null ? true : !Boolean.parseBoolean(data);
                this.isData = data == null ? true : Boolean.parseBoolean(data);
            }
            this.isIngest = false;
        } else {
            List<String> roles = (List<String>) map.get("roles");
            this.isClient = roles.contains("data") == false;
            this.isData = roles.contains("data");
            this.isIngest = roles.contains("ingest");
        }
        Map<String, Object> httpMap = (Map<String, Object>) map.get("http");
        if (httpMap != null) {
            String address = (String) httpMap.get("publish_address");
            if (address != null) {
                // if network.publish_host is set to hostname like localhost,
                // the publish_address contains hostname like "localhost/127.0.0.1:9200"
                address = address.substring(address.lastIndexOf('/') + 1);
                String[] scratch = address.split(":");
                this.publishAddress = new TNetworkAddress((httpSslEnabled ? "https://" : "") + scratch[0], Integer.parseInt(scratch[1]));
                this.hasHttp = true;
            } else {
                this.publishAddress = null;
                this.hasHttp = false;
            }
        } else {
            this.publishAddress = null;
            this.hasHttp = false;
        }

        Map<String, Object> attributesMap = (Map<String, Object>) map.get("attributes");
        if (attributesMap != null) {
            String thriftPortStr = (String) attributesMap.get("thrift_port");
            if (thriftPortStr != null) {
                try {
                    int thriftPort = Integer.valueOf(thriftPortStr);
                    hasThrift = true;
                    thriftAddress = new TNetworkAddress(this.ip, thriftPort);
                } catch (Exception e) {
                    hasThrift = false;
                }
            } else {
                hasThrift = false;
            }
        } else {
            hasThrift = false;
        }
    }

    public EsNodeInfo(String id, String seed) {
        this.id = id;
        String[] scratch = seed.split(":");
        int port = 80;
        if (scratch.length == 3) {
            String portStr = scratch[2];
            if (portStr.contains("/")) {
                portStr = portStr.substring(0, portStr.indexOf('/'));
            }
            port = Integer.parseInt(portStr);
        }
        String remoteHost = scratch[0] + ":" + scratch[1];
        this.name = remoteHost;
        this.host = remoteHost;
        this.ip = remoteHost;
        this.isClient = true;
        this.isData = true;
        this.isIngest = true;
        this.publishAddress = new TNetworkAddress(remoteHost, port);
        this.hasHttp = true;
    }

    public boolean hasHttp() {
        return hasHttp;
    }

    public boolean isClient() {
        return isClient;
    }

    public boolean isData() {
        return isData;
    }

    public boolean isIngest() {
        return isIngest;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public TNetworkAddress getPublishAddress() {
        return publishAddress;
    }

    public boolean isHasThrift() {
        return hasThrift;
    }

    public TNetworkAddress getThriftAddress() {
        return thriftAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EsNodeInfo nodeInfo = (EsNodeInfo) o;

        if (hasHttp != nodeInfo.hasHttp) {
            return false;
        }
        if (isClient != nodeInfo.isClient) {
            return false;
        }
        if (isData != nodeInfo.isData) {
            return false;
        }
        if (!id.equals(nodeInfo.id)) {
            return false;
        }
        if (!name.equals(nodeInfo.name)) {
            return false;
        }
        if (!host.equals(nodeInfo.host)) {
            return false;
        }
        if (!ip.equals(nodeInfo.ip)) {
            return false;
        }
        if (hasThrift != nodeInfo.hasThrift) {
            return false;
        }
        return (Objects.equals(publishAddress, nodeInfo.publishAddress))
                && (Objects.equals(thriftAddress, nodeInfo.thriftAddress));
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + host.hashCode();
        result = 31 * result + ip.hashCode();
        result = 31 * result + (publishAddress != null ? publishAddress.hashCode() : 0);
        result = 31 * result + (thriftAddress != null ? thriftAddress.hashCode() : 0);
        result = 31 * result + (hasHttp ? 1 : 0);
        result = 31 * result + (hasThrift ? 1 : 0);
        result = 31 * result + (isClient ? 1 : 0);
        result = 31 * result + (isData ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "EsNodeInfo{"
                + "id='" + id + '\''
                + ", name='" + name + '\''
                + ", host='" + host + '\''
                + ", ip='" + ip + '\''
                + ", publishAddress=" + publishAddress
                + ", hasHttp=" + hasHttp
                + ", isClient=" + isClient
                + ", isData=" + isData
                + ", isIngest=" + isIngest
                + ", hasThrift=" + hasThrift
                + ", thriftAddress=" + thriftAddress
                + '}';
    }
}
