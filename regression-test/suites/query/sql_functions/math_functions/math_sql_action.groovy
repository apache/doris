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

suite("sql_action", "math") {

	sql "set enable_vectorized_engine = true"
	sql "set batch_size = 4096"
	    
	// conv
	def result1 = sql "SELECT CONV(15,10,2)"
	assertTrue(result1.size() == 1)
	assertTrue(result1[0].size() == 1)
	assertTrue(result1[0][0] == '1111', "Conv result is 1")

	def result2 = sql "SELECT CONV('ff',16,10)"
	assertTrue(result2.size() == 1)
	assertTrue(result2[0].size() == 1)
	assertTrue(result2[0][0] == '255', "Conv result is 1")

	def result3 = sql "SELECT CONV(230,10,16)"
	assertTrue(result3.size() == 1)
	assertTrue(result3[0].size() == 1)
	assertTrue(result3[0][0] == 'E6', "Conv result is 1")
    	
    	// pmod
    	def result4 = sql "SELECT PMOD(13,5)"
	assertTrue(result4.size() == 1)
	assertTrue(result4[0].size() == 1)
	assertTrue(result4[0][0] == 3, "pmod result is 1")
	
	def result5 = sql "SELECT PMOD(-13,5)"
	assertTrue(result5.size() == 1)
	assertTrue(result5[0].size() == 1)
	assertTrue(result5[0][0] == 2, "pmod result is 1")
    	
}
