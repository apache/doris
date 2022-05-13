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

package org.apache.doris.persist;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

// for persist global variables
public class GlobalVarPersistInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(GlobalVarPersistInfo.class);
    // current default session variable when writing the edit log
    private SessionVariable defaultSessionVariable;
    // variable names which are modified
    private List<String> varNames;

    // the modified variable info will be saved as a json string
    private String persistJsonString;

    private GlobalVarPersistInfo() {
        // for persist
    }

    public GlobalVarPersistInfo(SessionVariable defaultSessionVariable, List<String> varNames) {
        this.defaultSessionVariable = defaultSessionVariable;
        this.varNames = varNames;
    }

    public String getPersistJsonString() {
        return persistJsonString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            JSONObject root = new JSONObject();
            for (String varName : varNames) {
                // find attr in defaultSessionVariable or GlobalVariables
                Object varInstance = null;
                Field theField = null;
                boolean found = false;
                // 1. first find in defaultSessionVariable
                for (Field field : SessionVariable.class.getDeclaredFields()) {
                    VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
                    if (attr == null) {
                        continue;
                    }
                    if (attr.name().equalsIgnoreCase(varName)) {
                        varInstance = this.defaultSessionVariable;
                        theField = field;
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    // find in GlobalVariables
                    for (Field field : GlobalVariable.class.getDeclaredFields()) {
                        VariableMgr.VarAttr attr = field.getAnnotation(VariableMgr.VarAttr.class);
                        if (attr == null) {
                            continue;
                        }

                        if (attr.name().equalsIgnoreCase(varName)) {
                            found = true;
                            varInstance = null;
                            theField = field;
                            break;
                        }
                    }
                }
                Preconditions.checkState(found, varName);

                theField.setAccessible(true);
                String fieldName = theField.getAnnotation(VariableMgr.VarAttr.class).name();
                switch (theField.getType().getSimpleName()) {
                    case "boolean":
                        root.put(fieldName, (Boolean) theField.get(varInstance));
                        break;
                    case "int":
                        root.put(fieldName, (Integer) theField.get(varInstance));
                        break;
                    case "long":
                        root.put(fieldName, (Long) theField.get(varInstance));
                        break;
                    case "float":
                        root.put(fieldName, (Float) theField.get(varInstance));
                        break;
                    case "double":
                        root.put(fieldName, (Double) theField.get(varInstance));
                        break;
                    case "String":
                        root.put(fieldName, (String) theField.get(varInstance));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + theField.getType().getSimpleName());
                }
            } // end for all variables

            Text.writeString(out, root.toString());
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
    }

    public static GlobalVarPersistInfo read(DataInput in) throws IOException {
        GlobalVarPersistInfo info = new GlobalVarPersistInfo();
        info.persistJsonString = Text.readString(in);
        return info;
    }
}
