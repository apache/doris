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
# encoding=utf-8 vi:ts=4:sw=4:expandtab:ft=python
"""
execute test case
date: 2018-07-24 17:16:49
"""
"""
case demo:
def test_sample_1():
    ret = execute('sample')
    assert ret


def test_sample():
    ret = implement.run_check('%s/sql/sample.sql' % file_dir, '%s/result/sample.result' % file_dir)
    assert ret
"""
import os
import pytest
import implement


file_dir = os.path.abspath(os.path.dirname(__file__)) 
input_dir = '%s/sql/' % file_dir
check_dir = '%s/result/' % file_dir


def setup_module():
    """
    setup
    """
    if implement.check_cluster() == False:
        raise pytest.skip("there is no enough be")


def execute(case):
    """ execute
    """
    test_file = input_dir + case + '.sql'
    result_file = check_dir + case + '.result'
    ret = implement.run_check(test_file=test_file, result_file=result_file)
    return ret


def test_issue_2932():
    """issue_2932"""
    ret = execute('issue_2932')
    assert ret


def test_issue_4772():
    """issue_4772"""
    ret = execute('issue_4772')
    assert ret


def test_issue_4598():
    """issue_4598"""
    ret = execute('issue_4598')
    assert ret


def test_issue_4281():
    """issue_4281"""
    ret = execute('issue_4281')
    assert ret


def test_issue_4167():
    """issue_4167"""
    ret = execute('issue_4167')
    assert ret


def test_issue_4361():
    """issue_4361"""
    ret = execute('issue_4361')
    assert ret


def test_issue_4451():
    """issue_4451"""
    ret = execute('issue_4451')
    assert ret


def test_issue_4544():
    """issue_4544"""
    ret = execute('issue_4544')
    assert ret


def test_issue_4716():
    """issue_4716"""
    ret = execute('issue_4716')
    assert ret


def test_issue_4720():
    """issue_4720"""
    ret = execute('issue_4720')
    assert ret


def test_issue_4806():
    """issue_4806"""
    ret = execute('issue_4806')
    assert ret


def test_issue_3008():
    """issue_3008"""
    ret = execute('issue_3008')
    assert ret


def test_issue_3275():
    """issue_3275"""
    ret = execute('issue_3275')
    assert ret


def test_issue_3792():
    """issue_3792"""
    ret = execute('issue_3792')
    assert ret


def test_issue_3828():
    """issue_3828"""
    ret = execute('issue_3828')
    assert ret


def test_issue_3854():
    """issue_3854"""
    ret = execute('issue_3854')
    assert ret


def test_issue_3942():
    """issue_3942"""
    ret = execute('issue_3942')
    assert ret


def test_issue_4093():
    """issue_4093"""
    ret = execute('issue_4093')
    assert ret


def test_issue_4218():
    """issue_4218"""
    ret = execute('issue_4218')
    assert ret


def test_issue_4241():
    """issue_4241"""
    ret = execute('issue_4241')
    assert ret


def test_issue_4278():
    """issue_4278"""
    ret = execute('issue_4278')
    assert ret


def test_issue_3494():
    """issue_3494"""
    ret = execute('issue_3494')
    assert ret


def test_issue_3520():
    """issue_3520"""
    ret = execute('issue_3520')
    assert ret


def test_issue_3527():
    """issue_3527"""
    ret = execute('issue_3527')
    assert ret


def test_issue_4581():
    """issue_4581"""
    ret = execute('issue_4581')
    assert ret


def test_issue_3771():
    """issue_3771"""
    ret = execute('issue_3771')
    assert ret


def test_issue_4975():
    """issue_4975"""
    ret = execute('issue_4975')
    assert ret


def test_issue_4926():
    """issue_4926"""
    ret = execute('issue_4926')
    assert ret


def test_issue_4833():
    """issue_4833"""
    ret = execute('issue_4833')
    assert ret


def test_issue_4971():
    """issue_4971"""
    ret = execute('issue_4971')
    assert ret


def test_issue_5430():
    """issue_5430"""
    ret = execute('issue_5430')
    assert ret


def test_issue_5440():
    """issue_5440"""
    ret = execute('issue_5440')
    assert ret


def test_issue_5284():
    """issue_5284"""
    ret = execute('issue_5284')
    assert ret


def test_issue_5355():
    """issue_5355"""
    ret = execute('issue_5355')
    assert ret


def test_issue_5121():
    """issue_5121"""
    ret = execute('issue_5121')
    assert ret


def test_issue_5346():
    """issue_5346"""
    ret = execute('issue_5346')
    assert ret


def test_issue_5188():
    """issue_5188"""
    ret = execute('issue_5188')
    assert ret


def test_issue_5138():
    """issue_5138"""
    ret = execute('issue_5138')
    assert ret


def test_issue_5171():
    """issue_5171"""
    ret = execute('issue_5171')
    assert ret


def test_issue_4546():
    """issue_4546"""
    ret = execute('issue_4546')
    assert ret


def test_issue_4313():
    """issue_4313"""
    ret = execute('issue_4313')
    assert ret


def test_issue_4040():
    """issue_4040"""
    ret = execute('issue_4040')
    assert ret


def test_issue_4483():
    """issue_4483"""
    ret = execute('issue_4483')
    assert ret


def test_issue_5109():
    """issue_5109"""
    ret = execute('issue_5109')
    assert ret


def test_issue_5144():
    """issue_5144"""
    ret = execute('issue_5144')
    assert ret


def test_issue_5483():
    """issue_5483"""
    assert execute('issue_5483')


def test_issue_5581():
    """issue_5581"""
    assert execute('issue_5581')


@pytest.mark.skip()
def test_issue_5583():
    """
    issue_5583 -> issue_12273
    case feature change
    """
    assert execute('issue_5583')


def test_issue_6095():
    """issue_6095"""
    assert execute('issue_6095')


def test_issue_6369():
    """issue_6369"""
    assert execute('issue_6369')


def test_issue_6767():
    """issue_6767"""
    assert execute('issue_6767')


def test_issue_7875():
    """issue_7875"""
    assert execute('issue_7875')


def test_issue_7664():
    """issue_7664"""
    assert execute('issue_7664')


def test_issue_7410():
    """issue 7410"""
    assert execute("issue_7410")


def test_issue_7374():
    """issue_7374"""
    assert execute("issue_7374")


def test_issue_7288():
    """issue_7288"""
    assert execute("issue_7288")


def test_issue_7106():
    """issue_7106"""
    assert execute("issue_7106")


def test_issue_8778():
    """issue_8778"""
    assert execute("issue_8778")


def test_issue_8850():
    """issue_8850"""
    assert execute("issue_8850")


