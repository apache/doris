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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * Author: Chenmingyu
 * Date: May 29, 2019
 */

public class SmallFileInfo implements Writable {
    public long dbId;
    public String catalogName;
    public String fileName;
    public String content;
    public long fileSize;
    public String md5;

    private SmallFileInfo() {

    }

    public SmallFileInfo(long dbId, String catalogName, String fileName, String content, long fileSize, String md5) {
        this.dbId = dbId;
        this.catalogName = catalogName;
        this.fileName = fileName;
        this.content = content;
        this.fileSize = fileSize;
        this.md5 = md5;
    }

    public SmallFileInfo(long dbId, String catalogName, String fileName) {
        this.dbId = dbId;
        this.catalogName = catalogName;
        this.fileName = fileName;
    }

    public static SmallFileInfo read(DataInput in) throws IOException {
        SmallFileInfo info = new SmallFileInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(dbId);
        Text.writeString(out, catalogName);
        Text.writeString(out, fileName);
        if (content != null) {
            out.writeBoolean(true);
            Text.writeString(out, content);
            out.writeLong(fileSize);
            Text.writeString(out, md5);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        catalogName = Text.readString(in);
        fileName = Text.readString(in);
        if (in.readBoolean()) {
            content = Text.readString(in);
            fileSize = in.readLong();
            md5 = Text.readString(in);
        }
    }
}
