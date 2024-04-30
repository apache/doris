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

package org.apache.doris.qe;

import org.apache.doris.common.ErrorCode;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlErrPacket;
import org.apache.doris.mysql.MysqlOkPacket;
import org.apache.doris.mysql.MysqlPacket;

// query state used to record state of query, maybe query status is better
public class QueryState {
    // Reused by arrow flight protocol
    public enum MysqlStateType {
        NOOP,   // send nothing to remote
        OK,     // send OK packet to remote
        EOF,    // send EOF packet to remote
        ERR,     // send ERROR packet to remote
        UNKNOWN  // send UNKNOWN packet to remote
    }

    public enum ErrType {
        ANALYSIS_ERR,
        OTHER_ERR
    }

    private MysqlStateType stateType = MysqlStateType.OK;
    private String errorMessage = "";
    private ErrorCode errorCode;
    private String infoMessage;
    private ErrType errType = ErrType.OTHER_ERR;
    private boolean isQuery = false;
    private long affectedRows = 0;
    private int warningRows = 0;
    // make it public for easy to use
    public int serverStatus = 0;
    public boolean isNereids = false;
    private ShowResultSet rs = null;

    public QueryState() {
    }

    public void reset() {
        stateType = MysqlStateType.OK;
        errorCode = null;
        infoMessage = null;
        errorMessage = "";
        serverStatus = 0;
        isQuery = false;
        affectedRows = 0;
        warningRows = 0;
        isNereids = false;
        rs = null;
    }

    public MysqlStateType getStateType() {
        return stateType;
    }

    public void setNoop() {
        stateType = MysqlStateType.NOOP;
    }

    public void setEof() {
        stateType = MysqlStateType.EOF;
    }

    public void setOk() {
        if (stateType == MysqlStateType.OK) {
            return;
        }
        setOk(0, 0, null);
    }

    public void setOk(long affectedRows, int warningRows, String infoMessage) {
        this.affectedRows = affectedRows;
        this.warningRows = warningRows;
        this.infoMessage = infoMessage;
        stateType = MysqlStateType.OK;
    }

    public void setError(String errorMsg) {
        this.setError(ErrorCode.ERR_UNKNOWN_ERROR, errorMsg);
    }

    public void setError(ErrorCode code, String msg) {
        this.stateType = MysqlStateType.ERR;
        this.errorCode = code;
        this.errorMessage = msg;
    }

    public void setMsg(String msg) {
        this.errorMessage = msg;
    }

    public void setErrType(ErrType errType) {
        this.errType = errType;
    }

    public ErrType getErrType() {
        return errType;
    }

    public void setIsQuery(boolean isQuery) {
        this.isQuery = isQuery;
    }

    public boolean isQuery() {
        return isQuery;
    }

    public String getInfoMessage() {
        return infoMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public int getWarningRows() {
        return warningRows;
    }

    public void setNereids(boolean nereids) {
        isNereids = nereids;
    }

    public boolean isNereids() {
        return isNereids;
    }

    public void setResultSet(ShowResultSet rs) {
        this.rs = rs;
    }

    public ShowResultSet getResultSet() {
        return this.rs;
    }

    public MysqlPacket toResponsePacket() {
        MysqlPacket packet = null;
        switch (stateType) {
            case OK:
                packet = new MysqlOkPacket(this);
                break;
            case EOF:
                packet = new MysqlEofPacket(this);
                break;
            case ERR:
                packet = new MysqlErrPacket(this);
                break;
            default:
                break;
        }
        return packet;
    }

    @Override
    public String toString() {
        return String.valueOf(stateType);
    }
}
