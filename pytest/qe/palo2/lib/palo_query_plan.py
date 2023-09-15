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
@file: palo_query_plan.py.py
@time: 2019/10/23 19:24
@desc:
"""


class PlanInfo(object):
    """
    explain sql
            PLAN FRAGMENT 0：查询frag 0
            PLAN FRAGMENT 1: 查询frag 1
            PLAN FRAGMENT 2: 查询frag 2
            ...
    """
    def __init__(self, plan_info):
        self.plan_info = plan_info

    def get_one_fragment(self, frag_num):
        """获取某一个fragment里的内容，frag_num表示第几个fragment内容
        如frag_num=1，对应的是frag0 -->frag1之间的内容
        """
        fragment_list = []
        for i in range(len(self.plan_info)):
            if 'PLAN FRAGMENT' in str(self.plan_info[i]):
                fragment_list.append(i)

        fragment_count = len(fragment_list)
        assert frag_num <= fragment_count, "want frag_num %s <=  all fragment_count %s"\
            % (frag_num, fragment_count )
        begin_pos = 0
        end_pos = 0
        lenth = len(self.plan_info)
        if frag_num < fragment_count:
            begin_pos = frag_num - 1
            end_pos = frag_num
        # frag_num == fragment_count
        else:
            begin_pos = frag_num - 1
            fragment_list.append(lenth)
            end_pos = -1
        return self.plan_info[fragment_list[begin_pos]: fragment_list[end_pos]]

    def get_frag_aggregate(self, fragment):
        """获取某fragment里关于AGGREGATE的内容"""
        # TODO

    def get_frag_olapScanNode_predicates(self, fragment, scannode=1):
        """
        获取某fragment里OlapScanNode下面关于PREDICATES的内容
        scannode如果一个fragment中有多个scannode（self joina），指定某个scannode，而不是返回第一个
        """
        lenth = len(fragment)
        count = 0
        for i in range(lenth):
            if "OlapScanNode" in str(fragment[i]):
                for j in range(i + 1, lenth):
                    if 'PREDICATES' in str(fragment[j]).upper():
                        count += 1
                        if count == scannode:
                            return fragment[j]
                        else:
                            continue
                else:
                    print(fragment)
                    return None
        return None

    def get_frag_Select_predicates(self, fragment):
        """获取某fragment里select 段里关于PREDICATES的内容
        eg：
        3:SELECT
        predicates: <slot 8> > 8  
        tuple ids: 5 4
        """
        lenth = len(fragment)
        for i in range(lenth):
            if 'PREDICATES' in str(fragment[i]).upper():
                return fragment[i]
        return None

    def get_frag_hash_join(self, fragment, key=None):
        """获取某fragment里的HASH JOIN"""
        lenth = len(fragment)
        count = 0
        for record in fragment:
            if key in record[0]:
                print(record)
                return record[0].strip().split(': ')[1]
        return None

    def get_frag_table_function_node(self, fragment):
        """获取某fragment里的TABLE FUNCTION NODE"""
        lenth = len(fragment)
        for i in range(lenth):
            if 'TABLE FUNCTION:' in str(fragment[i]).upper():
                return fragment[i]
        return None

    def get_frag_empty_set(self, fragment):
        """
        获取某fragment中的emptyset
        4:EMPTYSET
            tuple ids: 0 1
        """
        lenth = len(fragment)
        for i in range(lenth):
            # enable vectorized engine is: VEMPTYSET
            # disable vectorized engine is: EMPTYSET
            if 'EMPTYSET' in str(fragment[i]):
                return fragment[i + 1]
        return None


