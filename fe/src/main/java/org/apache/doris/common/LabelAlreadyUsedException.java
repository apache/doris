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

package org.apache.doris.common;

/*
 * Author: Chenmingyu
 * Date: Jan 16, 2019
 */

public class LabelAlreadyUsedException extends DdlException {

    private static final long serialVersionUID = -6798925248765094813L;

    public LabelAlreadyUsedException(String label) {
        super("Label [" + label + "] has already been used.");
    }

    public LabelAlreadyUsedException(String label, String subLabel) {
        super("Sub label [" + subLabel + "] has already been used.");
    }
}
