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

package org.apache.doris.load;

import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class Source implements Writable {
    private static final String DEFAULT_COLUMN_SEPARATOR = "\t";
    private static final String DEFAULT_LINE_DELIMITER = "\n";
    
    private List<String> fileUrls;
    private List<String> columnNames;
    private String columnSeparator;
    private String lineDelimiter;
    private boolean isNegative;
    private Map<String, Pair<String, List<String>>> columnToFunction;

    public Source() {
        this.fileUrls = new ArrayList<String>();
        this.columnNames = new ArrayList<String>();
        this.columnSeparator = DEFAULT_COLUMN_SEPARATOR;
        this.lineDelimiter = DEFAULT_LINE_DELIMITER;
        this.columnToFunction = Maps.newHashMap();
    }

    public Source(List<String> fileUrls, List<String> columnNames, String columnSeprator,
            String lineDelimiter, boolean isNegative) {
        this.fileUrls = fileUrls;
        if (fileUrls == null) {
            this.fileUrls = new ArrayList<String>();
        }
        this.columnNames = columnNames;
        if (columnNames == null) {
            this.columnNames = new ArrayList<String>();
        }
        this.columnSeparator = columnSeprator;
        this.lineDelimiter = lineDelimiter;
        this.isNegative = isNegative;
        this.columnToFunction = Maps.newHashMap();
    }

    public Source(List<String> fileUrls) {
        this(fileUrls, null, DEFAULT_COLUMN_SEPARATOR, DEFAULT_LINE_DELIMITER, false);
    }

    public List<String> getFileUrls() {
        return fileUrls;
    }

    public void setFileUrls(List<String> fileUrls) {
        this.fileUrls = fileUrls;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public String getColumnSeparator() {
        return columnSeparator;
    }

    public void setColumnSeparator(String columnSeparator) {
        this.columnSeparator = columnSeparator;
    }

    public String getLineDelimiter() {
        return lineDelimiter;
    }

    public void setLineDelimiter(String lineDelimiter) {
        this.lineDelimiter = lineDelimiter;
    }

    public boolean isNegative() {
        return isNegative;
    }

    public void setNegative(boolean isNegative) {
        this.isNegative = isNegative;
    }

    public Map<String, Pair<String, List<String>>> getColumnToFunction() {
        return columnToFunction;
    }

    public void setColumnToFunction(Map<String, Pair<String, List<String>>> columnToFunction) {
        this.columnToFunction = columnToFunction;
    }

    public void write(DataOutput out) throws IOException {
        int count = 0;
        if (fileUrls == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = fileUrls.size();
            out.writeInt(count);
            for (String url : fileUrls) {
                Text.writeString(out, url);
            }
        }
        
        if (columnNames == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = columnNames.size();
            out.writeInt(count);
            for (String name : columnNames) {
                Text.writeString(out, name);
            }
        }
        
        Text.writeString(out, columnSeparator);
        Text.writeString(out, lineDelimiter);
        out.writeBoolean(isNegative);
        
        if (columnToFunction == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            count = columnToFunction.size();
            out.writeInt(count);
            for (Entry<String, Pair<String, List<String>>> entry : columnToFunction.entrySet()) {
                Text.writeString(out, entry.getKey());
                Pair<String, List<String>> functionPair = entry.getValue();
                Text.writeString(out, functionPair.first);
                count = functionPair.second.size();
                out.writeInt(count);
                for (String arg : functionPair.second) {
                    if (arg == null) {
                        out.writeBoolean(false);
                    } else {
                        out.writeBoolean(true);
                        Text.writeString(out, arg);
                    }
                }
            }
        }
    }
    public void readFields(DataInput in) throws IOException {
        int count = 0;
        
        if (in.readBoolean()) {
            count = in.readInt();
            for (int i = 0; i < count; i++) {
                fileUrls.add(Text.readString(in).intern());
            }
        }
        
        if (in.readBoolean()) {
            count = in.readInt();
            for (int i = 0; i < count; i++) {
                columnNames.add(Text.readString(in).intern());
            }
        }

        columnSeparator = Text.readString(in).intern();
        lineDelimiter = Text.readString(in).intern();
        isNegative = in.readBoolean();
        
        if (in.readBoolean()) {
            count = in.readInt();
            for (int i = 0; i < count; i++) {
                String column = Text.readString(in).intern();
                String functionName = Text.readString(in).intern();
                int argsNum = in.readInt();
                List<String> args = Lists.newArrayList();
                for (int j = 0; j < argsNum; j++) {
                    if (in.readBoolean()) {
                        args.add(Text.readString(in));
                    }
                }
                columnToFunction.put(column, new Pair<String, List<String>>(functionName, args));
            }
        }
    }
    
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        
        if (!(obj instanceof Source)) {
            return false;
        }
        
        Source source = (Source) obj;
        
        // Check fileUrls
        if (fileUrls != source.fileUrls) {
            if (fileUrls == null || source.fileUrls == null) {
                return false;
            }
            if (fileUrls.size() != source.fileUrls.size()) {
                return false;
            }
            for (String url : fileUrls) {
                if (!source.fileUrls.contains(url)) {
                    return false;
                }
            }
        }
        
        // Check columnNames
        if (columnNames != source.columnNames) {
            if (columnNames == null || source.columnNames == null) {
                return false;
            }
            if (columnNames.size() != source.columnNames.size()) {
                return false;
            }
            for (String column : columnNames) {
                if (!source.columnNames.contains(column)) {
                    return false;
                }
            }
        }
        
        // columnToFunction
        if (columnToFunction != source.columnToFunction) {
            if (columnToFunction == null || source.columnToFunction == null) {
                return false;
            }
            if (columnToFunction.size() != source.columnToFunction.size()) {
                return false;
            }
            for (Entry<String, Pair<String, List<String>>> entry : columnToFunction.entrySet()) {
                String column = entry.getKey();
                if (!source.columnToFunction.containsKey(column)) {
                    return false;
                }
                if (!entry.getValue().equals(source.columnToFunction.get(column))) {
                    return false;
                }
            }
        }
        
        return columnSeparator.equals(source.columnSeparator)
                && lineDelimiter.equals(source.lineDelimiter)
                && isNegative == source.isNegative;
    }
    
    public int hashCode() {
        if (fileUrls == null || columnNames == null) {
            return -1;
        }
        
        int ret = fileUrls.size() ^ columnNames.size() ^ columnToFunction.size();
        ret ^= columnSeparator.length();
        ret ^= lineDelimiter.length();
        return ret;
    }
}
