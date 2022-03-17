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

#include "agent/topic_subscriber.h"
#include "common/logging.h"
#include "util/mutex.h"

namespace doris {

TopicSubscriber::TopicSubscriber() {}

TopicSubscriber::~TopicSubscriber() {
    // Delete all listeners in the register
    std::map<TTopicType::type, std::vector<TopicListener*>>::iterator it =
            _registered_listeners.begin();
    for (; it != _registered_listeners.end(); ++it) {
        std::vector<TopicListener*>& listeners = it->second;
        std::vector<TopicListener*>::iterator listener_it = listeners.begin();
        for (; listener_it != listeners.end(); ++listener_it) {
            delete *listener_it;
        }
    }
}

void TopicSubscriber::register_listener(TTopicType::type topic_type, TopicListener* listener) {
    // Unique lock here to prevent access to listeners
    WriteLock lock(_listener_mtx);
    this->_registered_listeners[topic_type].push_back(listener);
}

void TopicSubscriber::handle_updates(const TAgentPublishRequest& agent_publish_request) {
    // Shared lock here in order to avoid updates in listeners' map
    ReadLock lock(_listener_mtx);
    // Currently, not deal with protocol version, the listener should deal with protocol version
    const std::vector<TTopicUpdate>& topic_updates = agent_publish_request.updates;
    std::vector<TTopicUpdate>::const_iterator topic_update_it = topic_updates.begin();
    for (; topic_update_it != topic_updates.end(); ++topic_update_it) {
        std::vector<TopicListener*>& listeners = this->_registered_listeners[topic_update_it->type];
        std::vector<TopicListener*>::iterator listener_it = listeners.begin();
        // Send the update to all listeners with protocol version.
        for (; listener_it != listeners.end(); ++listener_it) {
            (*listener_it)->handle_update(agent_publish_request.protocol_version, *topic_update_it);
        }
    }
}
} // namespace doris
