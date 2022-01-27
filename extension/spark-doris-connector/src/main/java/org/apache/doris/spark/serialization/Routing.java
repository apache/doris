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

package org.apache.doris.spark.serialization;

import static org.apache.doris.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;

import org.apache.doris.spark.exception.IllegalArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * present an Doris BE address.
 */
public class Routing {
    private final static Logger logger = LoggerFactory.getLogger(Routing.class);

    private String host;
    private int port;

    public Routing(String routing) throws IllegalArgumentException {
        parseRouting(routing);
    }

    private void parseRouting(String routing) throws IllegalArgumentException {
        logger.debug("Parse Doris BE address: '{}'.", routing);
        String[] hostPort = routing.split(":");
        if (hostPort.length != 2) {
            logger.error("Format of Doris BE address '{}' is illegal.", routing);
            throw new IllegalArgumentException("Doris BE", routing);
        }
        this.host = hostPort[0];
        try {
            this.port = Integer.parseInt(hostPort[1]);
        } catch (NumberFormatException e) {
            logger.error(PARSE_NUMBER_FAILED_MESSAGE, "Doris BE's port", hostPort[1]);
            throw new IllegalArgumentException("Doris BE", routing);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        return "Doris BE{" +
                "host='" + host + '\'' +
                ", port=" + port +
                '}';
    }
}
