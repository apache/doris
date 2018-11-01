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
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

// Alter table clause.
public class AlterClause implements Writable {
    public void analyze(Analyzer analyzer) throws AnalysisException {
        throw new NotImplementedException();
    }

    public Map<String, String> getProperties() {
        throw new NotImplementedException();
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public static AlterClause read(DataInput in) throws IOException {
        String className = Text.readString(in);
        if (className.startsWith("com.baidu.palo")) {
            // we need to be compatible with former class name
            className = className.replaceFirst("com.baidu.palo", "org.apache.doris");
        }
        AlterClause alterClause = null;
        try {
            Class<? extends AlterClause> derivedClass = (Class<? extends AlterClause>) Class.forName(className);
            alterClause = derivedClass.newInstance();
            Class[] paramTypes = { DataInput.class };
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = { in };
            readMethod.invoke(alterClause, params);

            return alterClause;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read AlterClause", e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }

}
