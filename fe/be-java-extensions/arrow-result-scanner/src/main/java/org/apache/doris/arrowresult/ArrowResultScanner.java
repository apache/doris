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

package org.apache.doris.arrowresult;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.TableSchema;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

public class ArrowResultScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(ArrowResultScanner.class);

    private final byte[] ticket;
    private final String locationUri;
    private final String ip;
    private final String arrowPort;
    private final String user;
    private final String password;

    private RootAllocator allocatorFE;
    private FlightStream stream;
    private FlightClient clientBE;
    private FlightSqlClient sqlClientBE;
    private FlightClient clientFE;
    private int columnCount = 0;

    private long appendArrowDataTimeNs = 0;

    private final ArrowResultColumnValue columnValue = new ArrowResultColumnValue();

    public ArrowResultScanner(int batchSize, Map<String, String> params) {
        String[] requiredFields = params.get("required_fields").split(",");
        String[] requiredTypes = params.get("columns_types").split("#");
        ColumnType[] columnTypes = new ColumnType[requiredTypes.length];
        for (int i = 0; i < requiredTypes.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], requiredTypes[i]);
        }
        initTableInfo(columnTypes, requiredFields, batchSize);

        this.ip = params.get("ip");
        this.arrowPort = params.get("arrow_port");
        this.ticket = Base64.getDecoder().decode(params.get("ticket"));
        this.locationUri = params.get("location_uri");
        this.user = params.get("user");
        this.password = params.get("password");

        if (LOG.isDebugEnabled()) {
            LOG.debug("ArrowResultScanner ip = " + ip
                    + " ; arrowPort = " + arrowPort
                    + " ; ticketBytes = " + java.util.Arrays.toString(ticket)
                    + " ; locationUri = " + locationUri
                    + " ; user = " + user);
        }
    }

    @Override
    public void open() throws IOException {
        try {
            Location location = new Location(new URI(locationUri));
            Ticket ticket = new Ticket(this.ticket);
            this.allocatorFE = new RootAllocator(Integer.MAX_VALUE);
            this.clientFE = FlightClient.builder(allocatorFE,
                new Location(
                        new URI("grpc", null, ip, Integer.parseInt(arrowPort), null, null, null)
                )
            ).build();
            this.clientBE = FlightClient.builder(allocatorFE, location).build();
            this.sqlClientBE = new org.apache.arrow.flight.sql.FlightSqlClient(clientBE);
            this.stream = sqlClientBE.getStream(ticket, clientFE.authenticateBasicToken(user, password).get());
            this.columnCount = stream.getSchema().getFields().size();
        } catch (Throwable e) {
            LOG.error("Failed to open ArrowResultScanner: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (stream != null) {
                stream.close();
            }
            if (sqlClientBE != null) {
                sqlClientBE.close();
            }
            if (clientFE != null) {
                clientFE.close();
            }
            if (clientBE != null) {
                clientBE.close();
            }
            if (allocatorFE != null) {
                allocatorFE.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        try {
            while (stream.next()) {
                long startTime = System.nanoTime();

                VectorSchemaRoot streamRoot = stream.getRoot();
                int valueCount = streamRoot.getVector(0).getValueCount();
                for (int colIndex = 0; colIndex < columnCount; ++colIndex) {
                    FieldVector vector = streamRoot.getVector(colIndex);
                    for (int rowNum = 0; rowNum < valueCount; rowNum++) {
                        Object value = vector.getObject(rowNum);
                        columnValue.setValue(value);
                        columnValue.setColumnType(types[colIndex]);
                        appendData(colIndex, columnValue);
                    }
                }

                appendArrowDataTimeNs += System.nanoTime() - startTime;
                rows += valueCount;
                if (rows >= batchSize) {
                    return rows;
                }
            }
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next batch of arrow result. "
                    + "ip: {}, arrowPort: {}, ticketBytes: {}, locationUri: {}, user: {}",
                    ip, arrowPort, java.util.Arrays.toString(ticket), locationUri, user, e);
            throw new IOException(e);
        }
        return rows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.singletonMap("timer:AppendArrowDataTimeNs", String.valueOf(appendArrowDataTimeNs));
    }
}
