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

package org.apache.doris.hplsql;

public class DorisConsole implements Console {

    private StringBuilder msg;

    private StringBuilder error;

    public DorisConsole() {
        msg = new StringBuilder();
        error = new StringBuilder();
    }

    public String getMsg() {
        return msg.toString();
    }

    public String getError() {
        return error.toString();
    }

    @Override
    public void print(String msg) {
        this.msg.append(msg);
    }

    @Override
    public void printLine(String msg) {
        this.msg.append(msg).append("\n");
    }

    @Override
    public void printError(String msg) {
        this.error.append(msg);
    }

    @Override
    public void reset() {
        msg = new StringBuilder();
        error = new StringBuilder();
    }
}
