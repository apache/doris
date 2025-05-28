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

suite("test_show_storage_vault_command", "query,storage_vault,cloud") {
    try {
        if (!isCloudMode() || !enableStoragevault())
            return;
        // Execute the SHOW STORAGE VAULT command and verify the output
        checkNereidsExecute("SHOW STORAGE VAULT")
        String tmp = sql """SHOW STORAGE VAULT"""
        log.info("Result of Show Vault is ", tmp)

        // Execute the SHOW STORAGE VAULTS command and verify the output
        checkNereidsExecute("SHOW STORAGE VAULTS")
    } catch (Exception e) {
        // Log any exceptions that occur during testing
        log.error("Failed to execute SHOW STORAGE VAULT command", e)
        throw e
    }
}
