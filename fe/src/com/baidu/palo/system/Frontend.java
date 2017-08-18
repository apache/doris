// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.system;

import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.ha.FrontendNodeType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frontend implements Writable {
    
    private FrontendNodeType role;
    private String host;
    private int port;
    
    public Frontend() {
        role = FrontendNodeType.UNKNOWN;
        host = "";
        port = 0;
    }
    
    public Frontend(FrontendNodeType role, String host, int port) {
        this.role = role;
        this.host = host;
        this.port = port;
    }
    
    public void setRole(FrontendNodeType role) {
        this.role = role;
    }
    
    public FrontendNodeType getRole() {
        return this.role;
    }
    
    public void setHost(String host) {
        this.host = host;
    }
    
    public String getHost() {
        return this.host;
    }
    
    public void setPort(int port) {
        this.port = port;
    }
    
    public int getPort() {
        return this.port;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, role.name());
        Text.writeString(out, host);
        out.writeInt(port);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        role = FrontendNodeType.valueOf(Text.readString(in));
        if (role == FrontendNodeType.REPLICA) {
            // this is for compatibility.
            // we changed REPLICA to FOLLOWER
            role = FrontendNodeType.FOLLOWER;
        }
        host = Text.readString(in);
        port = in.readInt();
    }
    
    public static Frontend read(DataInput in) throws IOException {
        Frontend frontend = new Frontend();
        frontend.readFields(in);
        return frontend;
    }
    
    public String toString() {
        return role.name() + " " + host + ":" + port;
    }
}
