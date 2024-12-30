/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("runtime_filter") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql """
        use ${db};
        set runtime_filter_mode=global;
        set runtime_filter_type=7;
        set disable_join_reorder=true;
        """

    // because both l_orderkey=1 and o_orderkey=1, no need to generate rf: o_orderkey->l_orderkey
    qt_no_rf """
        explain shape plan select count() from lineitem join orders on l_orderkey=o_orderkey where o_orderkey=1;
    """ 

    //  "l_orderkey=o_orderkey"  is not used to generate rf, but "l_suppkey=o_custkey" is used 
    qt_one_rf """
        explain shape plan select count() from lineitem join orders on l_orderkey=o_orderkey and l_suppkey=o_custkey where o_orderkey=1;
    """
}