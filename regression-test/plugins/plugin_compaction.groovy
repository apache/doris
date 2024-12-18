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

import org.codehaus.groovy.runtime.IOGroovyMethods
import org.awaitility.Awaitility

Suite.metaClass.getComactionStatus = { String backendIP, String backendPort, String tabletID ->
    def (code, out, err) = be_get_compaction_status(backendIP, backendPort, tabletID)
    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
    if (code != 0) {
        throw new IllegalArgumentException("get compaction status failed, code = ${code}")
    }
        
    def compactionStatus = parseJson(out.trim())
    if ("success" == compactionStatus.status.toLowerCase()) {
        return compactionStatus.run_status
    } else {
        return false
    }
}

Suite.metaClass.assertCompactionStatus = { String backendIP, String backendPort, String tabletID ->
    Awaitility.await().untilAsserted({
        assert getComactionStatus(backendIP, backendPort, tabletID)
    })
}

Suite.metaClass.assertCompactionStatusAtMost = { String backendIP, String backendPort, String tabletID, long t, TimeUnit tu ->
    Awaitility.await().atMost(t, tu).untilAsserted({
        assert getComactionStatus(backendIP, backendPort, tabletID)
    })
}
