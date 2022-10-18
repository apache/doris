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

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.qe.GlobalVariable;

import com.google.common.base.Strings;

public class PassVar {
    private String text;
    private boolean isPlain;
    private byte[] scrambled;

    public PassVar(String text, boolean isPlain) {
        this.text = text;
        this.isPlain = isPlain;
    }

    public void analyze() throws AnalysisException {
        if (!Strings.isNullOrEmpty(text)) {
            if (isPlain) {
                long validaPolicy = GlobalVariable.validatePasswordPolicy;
                if (validaPolicy != 0) {
                    // validate password
                    MysqlPassword.validatePlainPassword(validaPolicy, text);
                }
                // convert plain password to scramble
                scrambled = MysqlPassword.makeScrambledPassword(text);
            } else {
                scrambled = MysqlPassword.checkPassword(text);
            }
        } else {
            scrambled = new byte[0];
        }
    }

    public byte[] getScrambled() {
        return scrambled;
    }

    public String getText() {
        return text;
    }

    public boolean isPlain() {
        return isPlain;
    }
}
