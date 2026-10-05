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

package org.apache.doris.indexpolicy;

import org.apache.doris.common.DdlException;

import java.util.Map;
import java.util.Set;

public abstract class BasePolicyValidator implements PolicyPropertyValidator {
    protected final Set<String> allowedProperties;

    public BasePolicyValidator(Set<String> allowedProperties) {
        this.allowedProperties = allowedProperties;
    }

    @Override
    public void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Properties cannot be null");
        }

        for (String key : properties.keySet()) {
            if (!allowedProperties.contains(key)) {
                throw new DdlException(getTypeName() + " does not support parameter '" + key + "'. "
                        + "Allowed parameters: " + allowedProperties);
            }
        }

        validateSpecific(properties);
    }

    protected abstract String getTypeName();

    protected abstract void validateSpecific(Map<String, String> properties) throws DdlException;
}

