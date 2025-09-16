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
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class CreateDictionaryPersistInfo extends DictionaryPersistInfo {
    public CreateDictionaryPersistInfo(Dictionary dictionary) {
        super(dictionary);
    }

    public static CreateDictionaryPersistInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        Dictionary dictionary = GsonUtils.GSON.fromJson(json, Dictionary.class);
        return new CreateDictionaryPersistInfo(dictionary);
    }
}
