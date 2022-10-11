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

package com.alibaba.datax.plugin.writer.doriswriter;

import java.util.Map;

public class DorisCodecFactory {
    public DorisCodecFactory (){

    }
    public static DorisCodec createCodec( Keys writerOptions) {
        if ( Keys.StreamLoadFormat.CSV.equals(writerOptions.getStreamLoadFormat())) {
            Map<String, Object> props = writerOptions.getLoadProps();
            return new DorisCsvCodec (null == props || !props.containsKey("column_separator") ? null : String.valueOf(props.get("column_separator")));
        }
        if ( Keys.StreamLoadFormat.JSON.equals(writerOptions.getStreamLoadFormat())) {
            return new DorisJsonCodec (writerOptions.getColumns());
        }
        throw new RuntimeException("Failed to create row serializer, unsupported `format` from stream load properties.");
    }
}
