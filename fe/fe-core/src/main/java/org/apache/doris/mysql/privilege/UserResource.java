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

package org.apache.doris.mysql.privilege;

import org.apache.doris.common.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// This class is used for keeping backward compatible
@Deprecated
public class UserResource {
    public static void write(DataOutput out) throws IOException {
        // resouce count
        out.writeInt(0);
        // group count
        out.writeInt(0);
    }

    public static void readIn(DataInput in) throws IOException {
        int numResource = in.readInt();
        for (int i = 0; i < numResource; ++i) {
            in.readInt();
            in.readInt();
        }

        int numGroup = in.readInt();
        for (int i = 0; i < numGroup; ++i) {
            Text.readString(in);
            in.readInt();
        }
    }
}
