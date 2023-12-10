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

############################################################################
#
#   @file win_function.py
#   @date 2017-07-26 11:13:53
#   @brief This file is for window function check
#
#############################################################################

"""
windows function
"""
import os
import sys

CURRENT = -100


def check_bound(preceding, following):
    """check preceding and following"""
    if preceding is None:
        preceding = 0
    elif isinstance(preceding, int):
        pass
    elif preceding.upper() == 'UNBOUNDED':
        preceding = 0
    elif preceding.upper() == 'CURRENT':
        preceding = CURRENT
    else:
        return False, 'preceding'

    if following is None:
        following = 0
    elif isinstance(following, int):
        pass
    elif following.upper() == 'UNBOUNDED':
        following = -1
    elif following.upper() == 'CURRENT':
        following = CURRENT
    else:
        return False, 'following'

    return preceding, following


def win_sum(input_result, preceding, following, partition_index, order_index, 
            value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition and order
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: 'rows' | 'range'
    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
       order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        sum = 0

        for r in range(start_bound, end_bound + 1):
            sum = input_result[r][value_index] + sum

        if win_type.upper() == 'RANGE':
            if pre_data and pre_data[order_index] == current_data[order_index] and preceding == 0:
                pre_same_value_data = list(pre_data)
                count = 0
                while pre_same_value_data[order_index] == current_data[order_index]:
                    pre_same_value_data.pop()
                    pre_same_value_data.append(sum)
                    ret.pop()
                    pre_same_value_data = list(ret[-1])
                    count += 1
                current_data.append(sum)
                while count != 0:
                    ret.append(tuple(current_data))
                    count -= 1
            elif pre_data and pre_data[order_index] == current_data[order_index] and \
                 following == -1:
                current_data = ret[-1]
            else:
                current_data.append(sum)
        elif win_type.upper() == 'ROWS':
            current_data.append(sum)
        else:
            print('win sum false')
            return False

        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_max(input_result, preceding, following, partition_index, order_index, 
            value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition and order
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: 'rows' | 'range'

    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
       order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        max_value = -sys.maxsize + 1

        for r in range(start_bound, end_bound + 1):
            max_value = max(max_value, input_result[r][value_index])
        current_data.append(max_value)
        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_min(input_result, preceding, following, partition_index, order_index, 
            value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition and order
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: 'rows' | 'range'
    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
        order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        min_value = sys.maxsize

        for r in range(start_bound, end_bound + 1):
            min_value = min(min_value, input_result[r][value_index])
        current_data.append(min_value)
        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_count(input_result, preceding, following, partition_index, order_index, 
              value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition and order
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: 'rows' | 'range'
    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
       order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        count = 0

        for r in range(start_bound, end_bound + 1):
            if input_result[r][value_index] is not None:
                count += 1

        if win_type.upper() == 'RANGE':
            if pre_data and pre_data[order_index] == current_data[order_index] and preceding == 0:
                pre_same_value_data = list(pre_data)
                num = 0
                while pre_same_value_data[order_index] == current_data[order_index]:
                    pre_same_value_data.pop()
                    pre_same_value_data.append(count)
                    ret.pop()
                    pre_same_value_data = list(ret[-1])
                    num += 1
                current_data.append(count)
                while num != 0:
                    ret.append(tuple(current_data))
                    num -= 1
            elif pre_data and pre_data[order_index] == current_data[order_index] and \
                 following == -1:
                current_data = ret[-1]
            else:
                current_data.append(count)
        elif win_type.upper() == 'ROWS':
            current_data.append(count)
        else:
            print('win count false')
            return False

        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_avg(input_result, preceding, following, partition_index, order_index, 
            value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition and order
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: 'rows' | 'range'
    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
        order_index >= column_num or value_index < 0 or value_index >= column_num:
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following

        sum = 0.0
        count = 0
        for r in range(start_bound, end_bound + 1):
            sum = input_result[r][value_index] + sum
            count += 1
        if win_type.upper() == 'RANGE':
            if pre_data and pre_data[order_index] == current_data[order_index] and preceding == 0:
                pre_same_value_data = list(pre_data)
                # pre_same_value_data = pre_data
                num = 0
                while pre_same_value_data[order_index] == current_data[order_index]:
                    pre_same_value_data.pop()
                    pre_same_value_data.append(sum / count)
                    ret.pop()
                    pre_same_value_data = list(ret[-1])
                    num += 1
                current_data.append(sum / count)
                while num != 0:
                    ret.append(tuple(current_data))
                    num -= 1
            elif pre_data and pre_data[order_index] == current_data[order_index] and \
                 following == -1:
                current_data = ret[-1]
            else:
                current_data.append(sum / count)
        elif win_type.upper() == 'ROWS':
            current_data.append(sum / count)
        else:
            print('win sum false')
            return False
        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_first_value(input_result, preceding, following, partition_index, order_index, 
                    value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: no use
    Returns: False or output result
    """
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
        order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        min_order = sys.maxsize
        min_idx = start_bound
        for r in range(start_bound, end_bound + 1):
            if min_order > input_result[r][order_index]:
                min_order = input_result[r][order_index]
                min_idx = r
        first_value = input_result[min_idx][value_index]
        current_data.append(first_value)
        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_last_value(input_result, preceding, following, partition_index, order_index, 
                   value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        preceding: 'current' | 'unbounded' | int number > 0
        following:  'current' | 'unbounded' | int number > 0
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: value column location in input, only 1 partition column
        win_type: no use
    Returns: False or output result"""
    if input_result == ():
        return ()

    preceding, following = check_bound(preceding, following)
    if preceding is False:
        print('preceding/following is error')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
        order_index >= column_num or value_index < 0 or value_index >= column_num:
        print('partition_id/order_id/value_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    for cur in range(0, length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        if preceding == CURRENT:
            start_bound = cur
        elif preceding == 0 or cur - preceding < current_paritition_up:
            start_bound = current_paritition_up
        else:
            start_bound = cur - preceding

        if following == CURRENT:
            end_bound = cur
        elif following + cur >= current_paritition_bottom or following == -1:
            end_bound = current_paritition_bottom
        else:
            end_bound = cur + following
        max_order = - sys.maxsize - 1
        max_idx = start_bound
        for r in range(start_bound, end_bound + 1):
            if max_order < input_result[r][order_index]:
                max_order = input_result[r][order_index]
                max_idx = r
        last_value = input_result[max_idx][value_index]
        current_data.append(last_value)
        ret.append(tuple(current_data))
        pre_data = current_data

    return tuple(ret)


def win_rank(input_result, preceding, following, partition_index, order_index, 
             value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        preceding:  None
        following:  None
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: None
        win_type: None
    Returns: False or output result
    """
    if input_result == ():
        return ()

    if preceding is None and following is None and win_type is None and value_index is None:
        pass
    else:
        print('win func rank param wrong')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
       order_index >= column_num:
        print('partition_id/order_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    cur = 0
    while (cur >= 0) and (cur < length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        start_bound = current_paritition_up
        end_bound = current_paritition_bottom
        order_list = list()
        order_map = dict()
        for r in range(start_bound, end_bound + 1):
            order_list.append(input_result[r][order_index])
            order_map[r] = input_result[r][order_index]
        order_list = sorted(order_list)
        for id, v in order_map.items():
            location = 1
            for rank in order_list:
                if v == rank:
                    current_data = list(input_result[id])
                    current_data.append(location)
                    ret.append(tuple(current_data))
                    break
                location += 1
        cur = end_bound + 1
        pre_data = current_data

    return tuple(ret)


def win_dense_rank(input_result, preceding, following, partition_index, order_index, 
                   value_index, win_type):
    """
     Args:
        input_result: input should be ordered by partition
        preceding: None
        following:  None
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: None
        win_type: None
    Returns: False or output result
    """
    if input_result == ():
        return ()

    if preceding is None and following is None and value_index is None and win_type is None:
        pass
    else:
        print('win func rank param wrong')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
                    order_index >= column_num:
        print('partition_id/order_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    cur = 0
    while cur >= 0 and cur < length:
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        start_bound = current_paritition_up
        end_bound = current_paritition_bottom
        order_list = list()
        order_map = dict()
        for r in range(start_bound, end_bound + 1):
            order_list.append(input_result[r][order_index])
            order_map[r] = input_result[r][order_index]
        order_list = sorted(set(order_list))
        for id, v in order_map.items():
            location = 1
            for rank in order_list:
                if v == rank:
                    current_data = list(input_result[id])
                    current_data.append(location)
                    ret.append(tuple(current_data))
                    break
                location += 1
        cur = end_bound + 1
        pre_data = current_data

    return tuple(ret)


def win_row_number(input_result, preceding, following, partition_index, order_index, 
                   value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        preceding: None
        following:  None
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: None
        win_type: None
    Returns: False or output result"""
    if input_result == ():
        return ()

    if preceding is None and following is None and value_index is None and win_type is None:
        pass
    else:
        print('win func rank param wrong')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
                    order_index >= column_num:
        print('partition_id/order_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    cur = 0
    while cur >= 0 and cur < length:
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        start_bound = current_paritition_up
        end_bound = current_paritition_bottom
        order_list = list()
        order_map = dict()
        for r in range(start_bound, end_bound + 1):
            order_list.append(input_result[r][order_index])
            order_map[r] = input_result[r][order_index]
        order_list = sorted(order_list)
        location = 1
        for rank in order_list:
            for id, v in order_map.items():
                if v == rank:
                    current_data = list(input_result[id])
                    current_data.append(location)
                    ret.append(tuple(current_data))
                    break
            location += 1
        cur = end_bound + 1
        pre_data = current_data

    return tuple(ret)


def win_lag(input_result, offset, default, partition_index, order_index, value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        offset: lag offset
        default:  default value
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: None
        win_type:  None
    Returns:
    """
    if input_result == ():
        return ()
    
    if value_index is None and win_type is None:
        pass
    else:
        print('win func lag param wrong')
        return False

    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
       order_index >= column_num:
        print('partition_id/order_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    cur = 0
    while (cur >= 0) and (cur < length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        start_bound = current_paritition_up
        end_bound = current_paritition_bottom

        r = start_bound
        while (r >= start_bound) and (r <= end_bound):
            current_data = list(input_result[r])
            if r - offset < start_bound:
                current_data.append(default)
            else:
                current_data.append(input_result[r - offset][order_index])
            ret.append(tuple(current_data))
            r += 1
        cur = end_bound + 1
        pre_data = current_data

    return tuple(ret)


def win_lead(input_result, offset, default, partition_index, order_index, value_index, win_type):
    """
    Args:
        input_result: input should be ordered by partition
        offset: lead offset
        default:  default value
        partition_index: partition column loacation in input result, only 1 partition column
        order_index: order column location in input, only 1 partition column
        value_index: None
        win_type:  None
    Returns:
    """
    if input_result == ():
        print('input data is empty')
        return ()
    
    if value_index is None and win_type is None:
        pass
    else:
        print('win func lead param wrong')
        return False
    column_num = len(input_result[0])
    length = len(input_result)
    if partition_index < 0 or partition_index >= column_num or order_index < 0 or \
                    order_index >= column_num:
        print('partition_id/order_id is error')
        return False

    ret = list()
    partition_value = None
    current_paritition_up = 0
    pre_data = None
    cur = 0
    while (cur >= 0) and (cur < length):
        current_data = list(input_result[cur])
        partition_value = current_data[partition_index]
        if not pre_data:
            current_paritition_up = 0
        elif partition_value != pre_data[partition_index]:
            current_paritition_up = cur
        else:
            pass
        for i in range(cur, length):
            tmp = input_result[i][partition_index]
            if tmp == partition_value:
                pass
            else:
                break
        if i == length - 1:
            current_paritition_bottom = i
        else:
            current_paritition_bottom = i - 1

        start_bound = current_paritition_up
        end_bound = current_paritition_bottom

        r = start_bound
        while (r >= start_bound) and (r <= end_bound):
            current_data = list(input_result[r])
            if r > end_bound - offset:
                current_data.append(default)
            else:
                current_data.append(input_result[r + offset][order_index])
            ret.append(tuple(current_data))
            r += 1
        cur = end_bound + 1
        pre_data = current_data
        print(ret)
        print(cur)

    return tuple(ret)


if __name__ == '__main__':
    input1 = ((u'false', -2147483647), (u'false', 103), (u'false', 1001),
             (u'false', 1002), (u'false', 1002), (u'false', 3021), (u'false', 5014),
             (u'false', 2147483647), (u'true', -2147483647), (u'true', 1001),
             (u'true', 3021), (u'true', 3021), (u'true', 5014), (u'true', 25699),
             (u'true', 2147483647), (u'false12', 103), (u'false12', 1001),)
    output = win_sum(input1, 'unbounded', 'current', 0, 1, 1, 'range')
    print(output)
