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

suite("sql_action", "query") {

	sql "set enable_vectorized_engine = true"
	sql "set batch_size = 4096"
	    
	// conv
	qt_select1 "SELECT CONV(15,10,2)"
	qt_select2 "SELECT CONV('ff',16,10)"
	qt_select3 "SELECT CONV(230,10,16)"

    	
    	// pmod
    	qt_select4 "SELECT PMOD(13,5)"
	qt_select5 "SELECT PMOD(-13,5)"

    	
}
