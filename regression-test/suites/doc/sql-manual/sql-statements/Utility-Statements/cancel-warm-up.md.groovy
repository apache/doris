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

import org.junit.jupiter.api.Assertions;

suite("docs/sql-manual/sql-statements/Utility-Statements/cancel-warm-up.md") {

    if (!isCloudMode()) {
        return
    }

    def show = {
        multi_sql """
            SHOW WARM UP JOB;
        """
    }

    try {
        show()
        multi_sql """
            CANCEL WARM UP JOB WHERE id = 1;
        """
    } catch (Throwable t) {
        println("examples in docs/sql-manual/sql-statements/Utility-Statements/cancel-warm-up.md is expected to fail on bad job id")
    } finally {
        show()
    }
}
