import configparser
from fe_user_resolver import FeUserResolver

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

"""
@Author : wei.zhang
@Email: zhangweiwhim@gmail.com
@CreateTime : 2024/2/7 14:40
"""

if __name__ == '__main__':
    cf = configparser.ConfigParser()
    cf.read("./conf")
    fe_host = cf.get('cluster', 'fe_host')
    query_port = int(cf.get('cluster', 'query_port'))
    user = cf.get('cluster', 'user')
    query_pwd = cf.get('cluster', 'query_pwd')

    print("============= CONF =============")
    print("fe_host =", fe_host)
    print("fe_query_port =", query_port)
    print("user =", user)
    print("====================================")
    fe_meta = FeUserResolver(fe_host, query_port, user, query_pwd)
    fe_meta.init()
    fe_meta.print_list(False)  # True 显示为table 格式，False 为列表格式
