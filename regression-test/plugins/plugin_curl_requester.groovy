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

import org.apache.doris.regression.suite.Suite
import org.codehaus.groovy.runtime.IOGroovyMethods

Suite.metaClass.curl = { String method, String url /* param */-> 
    Suite suite = delegate as Suite
    if (method != "GET" && method != "POST")
    {
        throw new Exception(String.format("invalid curl method: %s", method))
    }
    if (url.isBlank())
    {
        throw new Exception("invalid curl url, blank")
    }
    
    Integer timeout = 10; // 10 seconds;

    String cmd = String.format("curl --max-time %d -X %s %s", timeout, method, url).toString()
    logger.info("curl cmd: " + cmd)
    def process = cmd.execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
    String out = process.getText()

    return [code, out, err]
}

logger.info("Added 'curl' function to Suite")


Suite.metaClass.show_be_config = { String ip, String port /*param */ ->
    return curl("GET", String.format("http://%s:%s/api/show_config", ip, port))
}

logger.info("Added 'show_be_config' function to Suite")

Suite.metaClass.be_get_compaction_status{ String ip, String port, String tablet_id  /* param */->
    return curl("GET", String.format("http://%s:%s/api/compaction/run_status?tablet_id=%s", ip, port, tablet_id))
}

logger.info("Added 'be_get_compaction_status' function to Suite")

Suite.metaClass.be_run_cumulative_compaction = { String ip, String port, String tablet_id  /* param */-> 
    return curl("POST", String.format("http://%s:%s/api/compaction/run?tablet_id=%s&compact_type=cumulative", ip, port, tablet_id))
}

logger.info("Added 'be_run_cumulative_compaction' function to Suite")

Suite.metaClass.be_run_full_compaction = { String ip, String port, String tablet_id  /* param */-> 
    return curl("POST", String.format("http://%s:%s/api/compaction/run?tablet_id=%s&compact_type=full", ip, port, tablet_id))
}

Suite.metaClass.be_run_full_compaction_by_table_id = { String ip, String port, String table_id  /* param */-> 
    return curl("POST", String.format("http://%s:%s/api/compaction/run?table_id=%s&compact_type=full", ip, port, table_id))
}

logger.info("Added 'be_run_full_compaction' function to Suite")
Suite.metaClass.update_be_config = { String ip, String port, String key, String value /*param */ ->
    return curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", ip, port, key, value))
}

logger.info("Added 'update_be_config' function to Suite")
