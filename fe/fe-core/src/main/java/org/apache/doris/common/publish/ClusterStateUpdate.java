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

import org.apache.doris.thrift.TAgentPublishRequest;
import org.apache.doris.thrift.TAgentServiceVersion;

import com.google.common.collect.Lists;

import java.util.List;

// State used to publish
public class ClusterStateUpdate {
    private final List<TopicUpdate> updates;

    // Called by builder.
    private ClusterStateUpdate(List<TopicUpdate> updates) {
        this.updates = updates;
    }

    public TAgentPublishRequest toThrift() {
        TAgentPublishRequest request = new TAgentPublishRequest();
        request.setProtocolVersion(TAgentServiceVersion.V1);
        for (TopicUpdate update : updates) {
            request.addToUpdates(update.toThrift());
        }
        return request;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<TopicUpdate> updates;

        public Builder() {
            updates = Lists.newArrayList();
        }

        public Builder addUpdate(TopicUpdate update) {
            updates.add(update);
            return this;
        }

        public ClusterStateUpdate build() {
            return new ClusterStateUpdate(updates);
        }
    }
}
