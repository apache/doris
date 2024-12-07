# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import env
import subprocess
import time

cmd = [f'{env.LOGSTASH_HOME}/bin/logstash', '-f', f'{env.TEST_CONF_DIR}/start.conf']
print(f'Running command: {cmd}')
try:
    p = subprocess.Popen(cmd, text=True)
    time.sleep(10)
    if p.poll() is not None:
        raise Exception(f'Failed to start logstash in {env.LOGSTASH_HOME}')
finally:
    p.kill()
    p.wait()
