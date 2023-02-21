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

suite("test_mysql_connection") {
    """mysql -uroot -P9030 -h127.0.0.1 -e 'show databases; status;'""".execute()
    """mysql --ssl-mode=DISABLE -uroot -P9030 -h127.0.0.1 -e 'show databases; status'""".execute()
    """mysql --ssl-mode=REQUIRED -uroot -P9030 -h127.0.0.1 --tls-version=TLSv1.2 -e 'show databases; status'""".execute()
    """mysql --ssl-mode=REQUIRED -uroot -P9030 -h127.0.0.1 --tls-version=TLSv1.3 -e 'show databases; status'""".execute()
}
