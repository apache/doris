#!/bin/env python
# -*- coding: utf-8 -*-
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
the data set for test load on palo
Date: 2015/03/25 11:48:41
"""
#create

column_list = [("k1", "tinyint"), \
	       ("k2", "smallint"), \
	       ("k3", "int"),\
	       ("k4", "bigint"), \
	       ("k5", "decimal(9, 3)"),\
	       ("k6", "char(5)"), \
	       ("k10", "date"), \
	       ("k11", "datetime"),\
	       ("k7", "varchar(20)"),\
	       ("k8", "double", "max"),\
	       ("k9", "float", "sum")
	       ]

column_list_overflow = [("k1", "tinyint"), \
	                ("k3", "int"),\
	                ("k4", "bigint"), \
	                ("k5", "decimal(9, 3)"),\
	                ("k6", "char(5)"), \
      	                ("k10", "date"), \
	                ("k11", "datetime"),\
	                ("k7", "varchar(20)"),\
	                ("k8", "double", "max"),\
	                ("k9", "float", "sum"), \
			("k2", "smallint", "sum")
	       ]

column_name_list = ["k1", "k2", "k3", "k4", "k5", "k6", "k10", "k11", "k7", "k8", "k9"]
rollup_column_name_list = ["k1", "k9"]
rollup_overflow_column_name_list = ["k6", "k2"]

storage_type = "column"

lack_column_list = [("k1", "tinyint"), \
	            ("k2", "smallint"), \
	            ("k3", "int"),\
		    ("k6", "char(5)"), \
		    ("k10", "date"), \
	            ("k11", "datetime"),\
	            ("k7", "varchar(20)"),\
	            ("k8", "double", "max")
		   ]

many_column_list = [("k1", "tinyint"), \
		    ("k5", "decimal(9, 3)"),\
	            ("k2", "smallint"), \
	            ("k3", "int"),\
		    ("k6", "char(5)"), \
		    ("k10", "date"), \
	            ("k4", "bigint"), \
	            ("k11", "datetime"),\
		    ("k12", "int", None, 100),\
	            ("k7", "varchar(20)"),\
	            ("k8", "double", "max"),\
	            ("k9", "float", "sum"), \
		    ("k13", "int", "min", 1)
	          ]

mix_column_list = [("k1", "tinyint"), \
		  ("k5", "decimal(9, 3)"),\
	          ("k2", "smallint"), \
	          ("k3", "int"),\
		  ("k6", "char(5)"), \
		  ("k10", "date"), \
	          ("k4", "bigint"), \
	          ("k11", "datetime"),\
	          ("k7", "varchar(20)"),\
	          ("k8", "double", "max"),\
	          ("k9", "float", "sum")
	          ]

