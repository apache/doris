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

import java.util.Collection;

import org.apache.doris.system.Backend;

// Response handler contain a listener
public class AckResponseHandler extends ResponseHandler {
    private Listener listener;

    public AckResponseHandler(Collection<Backend> nodes, Listener listener) {
        super(nodes);
        this.listener = listener;
    }

    @Override
    public void onResponse(Backend node) {
        super.onResponse(node);
        listener.onResponse(node);
    }

    @Override
    public void onFailure(Backend node, Throwable t) {
        super.onFailure(node, t);
        listener.onFailure(node, t);
    }
}
