// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

package com.baidu.palo.analysis;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.palo.catalog.AccessPrivilege;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.google.common.base.Strings;

public class AlterUserStmt extends DdlStmt {
    private String userName;
    private AlterUserClause clause;
    
    public AlterUserStmt(String userName, AlterClause clause) {
        this.userName = userName;
        this.clause = (AlterUserClause) clause;
    }
    
    private boolean hasRightToModify(Analyzer analyzer) {
        String user = analyzer.getUser();
        String toUser = userName;
        
        // own can modify own
        if (user.equals(toUser)) {
            return true;
        }
        
        // admin can modify all 
        if (analyzer.getCatalog().getUserMgr().isAdmin(user)) {
            return true;
        }
        
        // superuse can modify Ordinary user
        if (analyzer.getCatalog().getUserMgr().isSuperuser(user)
                && !analyzer.getCatalog().getUserMgr().isSuperuser(toUser)) {
            return true;
        }
        return false;
    }
    
    private void checkWhiteListSize(Analyzer analyzer) throws AnalysisException {
        if (clause.getAlterUserType() == AlterUserType.ADD_USER_WHITELIST) {
            try {
                if (analyzer.getCatalog().getUserMgr().getWhiteListSize(userName) > 20) {
                    throw new AnalysisException("whitelist size excced the max (20)");
                }
            } catch (DdlException e) {
                throw new AnalysisException(e.getMessage());
            }
        }
    }
    
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        // check toUser
        if (Strings.isNullOrEmpty(userName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "empty user");
        }
        userName = ClusterNamespace.getUserFullName(getClusterName(), userName);
        // check destination user if exists
        try {
            analyzer.getCatalog().getUserMgr().checkUserIfExist(userName);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
        
        // check destination user's whitelist if ecceed max value
        checkWhiteListSize(analyzer);
        
        // only write user can modify
        analyzer.checkPrivilege(analyzer.getDefaultDb(), AccessPrivilege.READ_WRITE);
            
        // check if has the right
        if (!hasRightToModify(analyzer)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ALTER CLUSTER");
        }
        
        // alter clause analysis
        clause.analyze(analyzer);        
    }
    
    public String getUser() {
        return userName;
    }
    
    public List<String> getHosts() {
        return clause.getHosts();
    }
   
    public List<String> getIps() {
        return clause.getIps();
    }

    public List<String> getStarIps() {
        return clause.getStarIps();
    }
    
    public AlterUserType getAlterUserType() {
        return clause.getAlterUserType();
    }
    
    @Override
    public String toSql() {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        throw new NotImplementedException();
    }


}
