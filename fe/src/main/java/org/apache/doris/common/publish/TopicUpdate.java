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

package org.apache.doris.common.publish;

import org.apache.doris.thrift.TTopicItem;
import org.apache.doris.thrift.TTopicType;
import org.apache.doris.thrift.TTopicUpdate;

import com.google.common.collect.Lists;

import java.util.List;

// A update for one topic
// updates contain all topic item
// deletes contain all topic need delete
// This is a general structure, can stands for a delta or total information;
// all explain depends type implementation.
public class TopicUpdate {
    private final TTopicType type;
    private List<TTopicItem> updates;
    private List<String> deletes;

    public TopicUpdate(TTopicType type) {
        this.type = type;
    }

    public void addUpdates(TTopicItem update) {
        if (updates == null) {
            updates = Lists.newArrayList();
        }
        updates.add(update);
    }

    public void addDelete(String key) {
        if (deletes == null) {
            deletes = Lists.newArrayList();
        }
        deletes.add(key);
    }

    public TTopicUpdate toThrift() {
        TTopicUpdate tUpdate = new TTopicUpdate(type);

        if (updates != null) {
            tUpdate.setUpdates(updates);
        }
        if (deletes != null) {
            for (String toDelete : deletes) {
                tUpdate.addToDeletes(toDelete);
            }
        }

        return tUpdate;
    }
}
