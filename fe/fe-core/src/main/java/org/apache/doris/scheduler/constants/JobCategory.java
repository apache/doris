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

package org.apache.doris.scheduler.constants;

import lombok.Getter;

/**
 * The job category is used to distinguish different types of jobs.
 */
public enum JobCategory {
    COMMON(1, "common"),
    SQL(2, "sql"),
    MTMV(3, "mtmv"),
    ;

    @Getter
    private int code;

    @Getter
    private String name;

    JobCategory(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public static JobCategory getJobCategoryByName(String name) {
        for (JobCategory jobCategory : JobCategory.values()) {
            if (jobCategory.name.equalsIgnoreCase(name)) {
                return jobCategory;
            }
        }
        throw new IllegalArgumentException("Unknown job category name: " + name);
    }
}
