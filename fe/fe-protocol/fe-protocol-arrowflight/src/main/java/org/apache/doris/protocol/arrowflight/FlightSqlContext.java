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

package org.apache.doris.protocol.arrowflight;

import org.apache.doris.protocol.ProtocolContext;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Flight SQL Protocol Context.
 * 
 * <p>Implements the ProtocolContext interface for Arrow Flight SQL connections.
 * This context maintains session state for Flight SQL clients.
 * 
 * @since 2.0.0
 */
public class FlightSqlContext implements ProtocolContext {
    
    private static final String PROTOCOL_NAME = "arrowflight";
    private static final AtomicLong CONNECTION_ID_GENERATOR = new AtomicLong(0);
    
    private final long connectionId;
    private final String remoteAddress;
    private final int remotePort;
    private String userName;
    private String database;
    private boolean authenticated;
    private final AtomicBoolean killed = new AtomicBoolean(false);
    private boolean sslEnabled;
    private long startTime;
    
    /**
     * Creates a new Flight SQL context.
     * 
     * @param remoteAddress client address
     * @param remotePort client port
     */
    public FlightSqlContext(String remoteAddress, int remotePort) {
        this.connectionId = CONNECTION_ID_GENERATOR.incrementAndGet();
        this.remoteAddress = remoteAddress;
        this.remotePort = remotePort;
        this.startTime = System.currentTimeMillis();
    }
    
    @Override
    public String getProtocolName() {
        return PROTOCOL_NAME;
    }
    
    @Override
    public long getConnectionId() {
        return connectionId;
    }
    
    @Override
    public String getRemoteIP() {
        return remoteAddress;
    }
    
    @Override
    public String getRemoteHostPortString() {
        return remoteAddress + ":" + remotePort;
    }
    
    @Override
    public String getUser() {
        return userName;
    }
    
    /**
     * Sets the authenticated user name.
     * 
     * @param userName user name
     */
    public void setUser(String userName) {
        this.userName = userName;
        this.authenticated = (userName != null);
    }
    
    @Override
    public String getDatabase() {
        return database;
    }
    
    @Override
    public void setDatabase(String database) {
        this.database = database;
    }
    
    @Override
    public boolean isAuthenticated() {
        return authenticated;
    }
    
    @Override
    public boolean isKilled() {
        return killed.get();
    }
    
    @Override
    public void setKilled() {
        killed.set(true);
    }
    
    @Override
    public boolean isSslEnabled() {
        return sslEnabled;
    }
    
    /**
     * Sets SSL enabled flag.
     * 
     * @param sslEnabled true if SSL is enabled
     */
    public void setSslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
    }
    
    @Override
    public void cleanup() {
        // Release any resources associated with this context
        killed.set(true);
    }
    
    @Override
    public long getStartTime() {
        return startTime;
    }
    
    @Override
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public <T> T getChannel() {
        // Flight SQL uses gRPC streams, not traditional channels
        return null;
    }
    
    @Override
    public String toString() {
        return "FlightSqlContext{" 
                + "connectionId=" + connectionId 
                + ", remote=" + getRemoteHostPortString()
                + ", user='" + userName + '\'' 
                + ", database='" + database + '\'' 
                + '}';
    }
}
