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

package org.apache.doris.resource.resourcegroup;

// used to mark QueryQueue offer result
// if offer failed, then need to cancel query
// and return failed reason to user client
public class QueueOfferToken {

    private Boolean offerResult;

    private String offerResultDetail;

    public QueueOfferToken(Boolean offerResult) {
        this.offerResult = offerResult;
    }

    public QueueOfferToken(Boolean offerResult, String offerResultDetail) {
        this.offerResult = offerResult;
        this.offerResultDetail = offerResultDetail;
    }

    public Boolean isOfferSuccess() {
        return offerResult;
    }

    public String getOfferResultDetail() {
        return offerResultDetail;
    }

}
