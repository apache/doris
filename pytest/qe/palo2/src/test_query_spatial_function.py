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
依赖mysql 5.6版本
"""
import sys
sys.path.append("../lib/")
from palo_qe_client import QueryBase
import query_util as util

sys.path.append("../../../lib/")
import palo_logger
LOG = palo_logger.Logger.getLogger()
L = palo_logger.StructedLogMessage

table_name = "test"
join_name = "baseall"
spatial_name = "spatial_table"


def setup_module():
    """
    init config
    """
    global runner
    runner = QueryBase()


def test_query_spatial_Polygon():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_Polygon",
    "describe": "test for spatial_Polygon；矩形图,类型；边界；函数，几个参数的特点；建表插入数据",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_Polygon；矩形图
    类型；边界；函数，几个参数的特点；建表插入数据
    """
    for index in range(11):
        line1 = "SELECT ST_AsText(ST_Polygon('POLYGON((0 0,k%s 10,0 10,10 0,0 0))')), k%s from \
        %s order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON((0 0,k%s 10,0 10,10 0,0 0))')), k%s from \
                %s order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        runner.check2(line1, line2)

    ##数据对 上限边界
    date_cup = ',(10 0, 10 10, 0 10, 0 0, 10 0)'
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON((10 0, 10 10, 0 10, 0 0, 10 0)"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON((10 0, 10 10, 0 10, 0 0, 10 0)"
    for i in range(3):
        line1 += date_cup
        line2 += date_cup
    line1 += ")'))"
    line2 += ")'))"
    print(line1, line2)
    runner.check2(line1, line2)

    ##函数 不支持
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0, 0 10+1, 10 10, 10 0,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON((0 0, 0 10+2, 10 10, 10 0,0 0))'))"
    runner.check2(line1, line2)
    ##两组数据不同,doris返回NULL
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0, 0 10, 10 10, 10 0,0 0),\
    (0 0, 0 30, 30 30, 30 0,0 0))'))"
    #line2 = "select ST_AsText(GEOMFROMTEXT('polygon((0 0,0 10,10 10,10 0,0 0), \
    #(0 0,0 30,30 30,30 0,0 0))'))"
    LOG.info(L('palo sql', palo_sql=line1))
    res = runner.query_palo.do_sql(line1)
    print(line1, res)
    excepted = ((None,),)
    util.check_same(res, excepted)
    ##数据对的大小关系,doris有要求 不能交叉
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0,0 10,10 10,10 0,2 11,0 0))'))"
    ###line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0,2 11,0 0))'))"
    LOG.info(L('palo sql', palo_sql=line1))
    res = runner.query_palo.do_sql(line1)
    print(line1, res)
    excepted = ((None,),)
    util.check_same(res, excepted)
    ##多组数据,>5组,doris会重新排序
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    #line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((-1 -1, 11 0, 11 2, 10 0, 10 10, 0 10, 0 0, -1 -1))'))"
    runner.check2(line1, line2)
    ##其他关键字
    line1 = "SELECT ST_AsText(ST_POLYFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_POLYGONFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_POLYGON('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_PolyFromText('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_PolygonFromText('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    ###数据对是负数
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, -2 -2,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, -2 -2,0 0))'))"
    runner.check2(line1, line2)
    ##数据对相邻会去重,mysql不去重
    line = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0, 10 0, 8 8, 8 8,8 8,0 10,-2 -2,0 0))'))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'POLYGON ((0 0, 10 0, 8 8, 0 10, -2 -2, 0 0))',),)
    util.check_same(res, excepted)
    #经纬度的极限,mysql没这个限制
    lat_lng_list = ['(0 0, 10 10, 80 91, 0 0)', '(0 0, 10 10, 181 89, 0 0)',\
        '(0 0, 10 10, 80 -91, 0 0)', '(0 0, 10 10, -181 89, 0 0)']
    for index in lat_lng_list:
        line1 = "SELECT ST_AsText(ST_Polygon('POLYGON(%s)'))" % (index,)
        LOG.info(L('palo sql', palo_sql=line1))
        res = runner.query_palo.do_sql(line1)
        print(line1, res)
        excepted = ((None,),)
        util.check_same(res, excepted)

    line = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0,'10.1' 10,0 10, 10 0,0 0))'))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(ST_Polygon('POLYGON (0 0,'10.1' 10,0 10, 10 0,0 0)'))"
    runner.checkwrong(line)
    ##建表
    line = "drop table if exists %s" % spatial_name
    runner.init(line)
    sql = 'create table %s(k1 tinyint, k2 int, k5 decimal(9,3) \
         NULL, k3 double sum, k4 float sum)\
         engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")'\
         % spatial_name
    msql = 'create table %s(k1 tinyint, k2 int, k5 decimal(9,3) NULL, k3 double NULL, k4 float NULL)'\
        % spatial_name
    runner.init(sql, msql)
    ##insert data
    len_data = 3
    for i in range(len_data):
        line = "insert into %s values (%s, %s, 115.2637, 39.2666, 117.2637)" % \
               (spatial_name, i, i + 2)
        runner.init(line)
    line1 = "SELECT ST_AsText(ST_Polygon('POLYGON ((0 0, k1 0, k1 k2, 0 k2, 0 0))'))\
           from %s order by k1" % spatial_name
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((0 0, k1 0, k1 k2, 0 k2, 0 0))'))\
            from %s order by k1" % spatial_name
    runner.check2(line1, line2)


def test_query_spatial_Line():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_Line",
    "describe": "test for spatial_Line；曲线，注意括号，不能多,类型；边界；函数，几个参数的特点；建表插入数据",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_Line；曲线，注意括号，不能多
    类型；边界；函数，几个参数的特点；建表插入数据
    """
    for index in range(11):
        line1 = "SELECT ST_AsText(ST_LineFromText('LINESTRING(0 0，k%s 10)')), k%s from \
        %s order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING(0 0，k%s 10)')), k%s from \
                %s order by k%s limit 1" % (index + 1, index + 1, join_name, index + 1)
        runner.check2(line1, line2)

    ##数据对 上限边界
    line1 = "SELECT ST_AsText(ST_LINEFROMTEXT('LINESTRING(10 0, 10 10, 0 10, 0 0, 10 0"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING(10 0, 10 10, 0 10, 0 0, 10 0"
    for i in range(30):
        line1 += ',%s %s' % (i, i+1)
        line2 += ',%s %s' % (i, i+1)
    line1 += ")'))"
    line2 += ")'))"
    print(line1, line2)
    runner.check2(line1, line2)

    ##函数 不支持
    line1 = "SELECT ST_AsText(ST_LINEFROMTEXT('LINESTRING (0 0, 0 10+1, 10 10, 10 0,0 0)'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING(0 0, 0 10+2, 10 10, 10 0,0 0)'))"
    runner.check2(line1, line2)
    ##两组数据返回NULL
    line1 = "SELECT ST_AsText(ST_LINEFROMTEXT('LINESTRING ((0 0, 0 10, 10 10, 10 0,0 0),\
    (0 0, 0 30, 30 30, 30 0,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING ((0 0, 0 10, 10 10, 10 0,0 0),\
        (0 0, 0 30, 30 30, 30 0,0 0))'))"
    runner.check2(line1, line2)
    ##多组数据
    line1 = "SELECT ST_AsText(ST_LINEFROMTEXT\
        ('LINESTRING (0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0)'))"
    #line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT\
        ('LINESTRING(0 0,0 10,10 10,10 0, 11 2,11 0 ,-1 -1,0 0)'))"
    runner.check2(line1, line2)
    ##其他关键字
    line1 = "SELECT ST_AsText(ST_LineStringFromText\
        ('LINESTRING(0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_LINEFROMTEXT\
       ('LINESTRING(0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_LINESTRINGFROMTEXT\
       ('LINESTRING(0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0)'))"
    runner.check2(line1, line2)

    ###数据对是负数
    line1 = "SELECT ST_AsText(ST_LineFromText('LINESTRING ((0 0, 10 0, 10 10, 0 10, -2 -2,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING((0 0, 10 0, 10 10, 0 10, -2 -2,0 0))'))"
    runner.check2(line1, line2)
    ##数据对相邻会去重,mysql不去重
    line = "SELECT ST_AsText(ST_LineFromText('LINESTRING (0 0, 10 0, 8 8, 8 8,8 8,0 10,-2 -2,0 0)'))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'LINESTRING (0 0, 10 0, 8 8, 0 10, -2 -2, 0 0)',),)
    util.check_same(res, excepted)
    #经纬度的极限,mysql没这个限制
    lat_lng_list = ['(0 0, 80 91)', '(0 0, 181 89)', '(0 0, 80 -91)', '(0 0, -181 89)']
    for index in lat_lng_list:
        line1 = "SELECT ST_AsText(ST_LineFromText('LINESTRING%s'))" % (index,)
        LOG.info(L('palo sql', palo_sql=line1))
        res = runner.query_palo.do_sql(line1)
        print(line1, res)
        excepted = ((None,),)
        util.check_same(res, excepted)

    line1 = "SELECT ST_AsText(ST_LineFromText('LINESTRING((0 0,\"10.1\" 10,0 10, 10 0,0 0))'))"
    line2 = "SELECT ST_AsText(GeomFromText('LINESTRING((0 0,\"10.1\" 10,0 10, 10 0,0 0))'))"
    runner.check2(line1, line2)
    line = "SELECT ST_AsText(ST_LineFromText('LINESTRING (0 0,'10.1' 10,0 10, 10 0,0 0)'))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(LINESTRINGFROMTEXT('LINESTRING (0 0,0 10)'))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(GeomFromText('LINESTRING(1 1,2 2,3 4, 2 3)'))"
    runner.checkwrong(line)
    ##建表
    line = "drop table if exists %s" % spatial_name
    runner.init(line)
    sql = 'create table %s(k1 tinyint, k2 int, k5 decimal(9,3) \
           NULL, k3 double sum, k4 float sum)\
           engine=olap distributed by hash(k1) buckets 5 properties("storage_type"="column")'\
          % spatial_name
    msql = 'create table %s(k1 tinyint, k2 int, k5 decimal(9,3) NULL, k3 double NULL, k4 float NULL)'\
        % spatial_name
    runner.init(sql, msql)
    ##insert data
    len_data = 3
    for i in range(len_data):
        line = "insert into %s values (%s, %s, 115.2637, 39.2666, 117.2637)" % \
               (spatial_name, i, i + 2)
        runner.init(line)
    line1 = "SELECT ST_AsText(ST_LineFromText('LINESTRING(0 0, k1 0, k1 k2, 0 k2, 0 0)'))\
           from %s order by k1" % spatial_name
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING(0 0, k1 0, k1 k2, 0 k2, 0 0)'))\
            from %s order by k1" % spatial_name
    runner.check2(line1, line2)


def test_query_spatial_point():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_point",
    "describe": "test for spatial_point；点,经纬度,类型；边界；函数，几个参数的特点；建表插入数据",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_point；点,经纬度
    类型；边界；函数，几个参数的特点；建表插入数据
    """
    for index in range(9):
        line1 = "SELECT ST_AsText(ST_Point(k%s,k%s)), k%s from  %s\
           where k%s >0 and k%s <90 order by k%s limit 1" % \
            (index + 1, index + 1, index + 1, join_name, index + 1, index + 1, index + 1)
        line2 = "SELECT ST_AsText(Point(k%s,k%s)), k%s from  %s\
           where k%s >0 and k%s <90 order by k%s limit 1" % \
            (index + 1, index + 1, index + 1, join_name, index + 1, index + 1, index + 1)
        runner.check2(line1, line2)
    ###精度 小数点9位
    line1 = "SELECT ST_AsText(ST_Point(123.12345678901234567890,89.1234567890))"
    line2 = "SELECT ST_AsText(Point(123.123456789,89.123456789))"
    runner.check2(line1, line2)
    ##函数 支持
    line1 = "SELECT ST_AsText(ST_Point(10/2,10%3))"
    line2 = "SELECT ST_AsText(Point(10/2, 10%3))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_Point(0+10,10-1))"
    line2 = "SELECT ST_AsText(Point(0+10,10-1))"
    runner.check2(line1, line2)

    line1 = "SELECT ST_AsText(ST_Point(repeat(0+9,2),23))"
    line2 = "SELECT ST_AsText(Point(repeat(0+9,2),23))"
    runner.check2(line1, line2)
    #经纬度的极限,mysql没这个限制
    lat_lng_list = [(80,91), (181,89), (80,-91), (-181,89)]
    for index in lat_lng_list:
        line1 = "SELECT ST_AsText(ST_Point%s)" % (index,)
        LOG.info(L('palo sql', palo_sql=line1))
        res = runner.query_palo.do_sql(line1)
        print(line1, res)
        excepted = ((None,),)
        util.check_same(res, excepted)
    ##其他关键字
    line1 = "SELECT ST_AsText(ST_POINT(repeat(0+9,2),23))"
    runner.check2(line1, line2)
    ###数据对是字符型,正负
    lat_lng_list = [('-80', 81), (18, '-89'), ('-80', '-71'), (-180, -90)]
    for index in lat_lng_list:
        line1 = "SELECT ST_AsText(ST_Point%s)" % (index,)
        line2 = "SELECT ST_AsText(Point%s)" % (index,)
        runner.check2(line1, line2)

    line = "SELECT ST_AsText(ST_Point(1,1,1))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(ST_Point(1,1), ST_Point(1,1))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(ST_Point((1,1),(1,2)))"
    runner.checkwrong(line)


def test_query_spatial_Circle():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_Circle",
    "describe": "test for spatial_Circle；经纬度，半径,类型；边界；函数",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_Circle；经纬度，半径
    类型；边界；函数
    """
    for index in range(11):
        line = "SELECT ST_AsText(ST_Circle(k%s, k%s, 10)),k%s from %s order by k%s limit 1" % \
               (index + 1, index + 1, index + 1, join_name, index + 1)
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        excepted = util.read_file('../data/query_spatial_circle.txt', index + 1)
        print("line %s, actual %s, excepted %s" % (line, res, excepted))
        if index not in [4, 9 , 10]:
            excepted = tuple(eval(excepted))
            util.check_same(res, excepted)
            continue
        
    line = "SELECT ST_AsText(ST_Circle(111, 64, 10000))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'CIRCLE ((111 64), 10000)',),)
    util.check_same(res, excepted)
    ##经纬度
    lat_lng_list = [80, 91, 181, 89, 80, -91, -181, 89]
    for index in range(0, len(lat_lng_list), 2):
        line1 = "SELECT ST_AsText(ST_Circle(%s, %s, 100))" % \
            (lat_lng_list[index], lat_lng_list[index + 1])
        LOG.info(L('palo sql', palo_sql=line1))
        res = runner.query_palo.do_sql(line1)
        print(line1, res)
        excepted = ((None,),) 
        util.check_same(res, excepted)

    ##边界 8位数
    line = "SELECT ST_AsText(ST_Circle(111, 64, 123456789))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((u'CIRCLE ((111 64), 20015118.2119)',),)
    util.check_same(res, excepted)
    #函数
    lat_lng_list = ['(111-23, 64+23, 10000-90)', '(11*10, 640/10, 10000%90)', '(10%10, 640%10, 10000%90)']
    res_list = [((u'CIRCLE ((88 87), 9910)',),), ((u'CIRCLE ((110 64), 10)',),), ((u'CIRCLE ((0 0), 10)',),)]
    for index in range(len(lat_lng_list)):
        line = "SELECT ST_AsText(ST_Circle%s)" % lat_lng_list[index]
        LOG.info(L('palo sql', palo_sql=line))
        res = runner.query_palo.do_sql(line)
        print("line %s, actual res %s, except res %s" % (line, res, res_list[index]))
        excepted = res_list[index]
        util.check_same(res, excepted)
    #checkwrong
    line = "SELECT ST_AsText(ST_Circle(36,36))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText(ST_Circle(36,36,36),1)"
    runner.checkwrong(line)

def test_query_spatial_astext_aswkt():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_astext_aswkt",
    "describe": "test for spatial_astext,mysql 5.6+ is SELECT ST_AsText(Point),各个类型；边界；错误语法",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_astext,mysql 5.6+ is SELECT ST_AsText(Point）
    各个类型；边界；错误语法
    """

    line1 = "SELECT ST_AsText(ST_Point(24.7, 56))"
    line2 = "SELECT ST_AsText(Point(24.7, 56))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsWKT(ST_Point(24.7, 56))"
    line2 = "SELECT ST_AsWKT(Point(24.7, 56))"
    runner.check2(line1, line2)
    ###嵌套是NULL
    line1 = "SELECT ST_AsWKT(ST_AsWKT(ST_Point('-80', 81)))"
    line2 = "SELECT ST_AsWKT(ST_AsWKT(Point('-80', 81)))"
    runner.check2(line1, line2)
    #checkwrong
    line = "SELECT ST_AsWKT((111, 64, 123456789))"
    runner.checkwrong(line)
    line = "SELECT ST_AsText((111, 64, 123456789))"
    runner.checkwrong(line)
    line = "SELECT STAsText(ST_Point(36,36,36))"
    runner.checkwrong(line)


def test_query_spatial_Contains():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_Contains",
    "describe": "test for spatial_Contains；几何图形里是否包含某点;mysql is ST_Contains(g1, g2),类型；边界；函数；包含点或线或圆或多边形",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_Contains；几何图形里是否包含某点;mysql is ST_Contains(g1, g2)???
    类型；边界；函数；包含点或线或圆或多边形
    """
    #多边形 点；多边形 线
    line1 = "SELECT ST_Contains(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Point(5, 5)),\
         ST_Contains(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), ST_Point(50, 50))"
    line2 = "SELECT ST_Contains(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), Point(5, 5)),\
             ST_Contains(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), Point(50, 50))"
    runner.check2(line1, line2)
    ##线在多边形上,里,外
    line1 = "SELECT ST_Contains(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), \
      ST_LineFromText('LINESTRING(0 0, 0 3)')), ST_Contains(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),\
     ST_LineFromText('LINESTRING(0 0, 3 3)')), ST_Contains(ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),\
     ST_LineFromText('LINESTRING(0 0, 13 3)'))"
    line2 = "SELECT ST_Contains(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), \
          GEOMFROMTEXT('LINESTRING(0 0, 0 3)')), ST_Contains(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),\
         GEOMFROMTEXT('LINESTRING(0 0, 3 3)')), ST_Contains(GEOMFROMTEXT('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),\
         GEOMFROMTEXT('LINESTRING(0 0, 13 3)'))"
    ####runner.check2(line1, line2)???
    #线,点 vs 线 线
    line1 = "SELECT ST_Contains(ST_LineFromText('LINESTRING(0 0, 0 3)'), ST_Point(0, 2))"
    line2 = "SELECT ST_Contains(GEOMFROMTEXT('LINESTRING(0 0, 0 3)'), Point(0, 2))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_Contains(ST_LineFromText('LINESTRING(0 0, 0 3)'), \
            ST_LineFromText('LINESTRING(0 0, 0 2)'))"
    line2 = "SELECT ST_Contains(GEOMFROMTEXT('LINESTRING(0 0, 0 3)'), \
        GEOMFROMTEXT('LINESTRING(0 0, 0 2)'))"
    runner.check2(line1, line2)
    ##圆 线 点
    line = "SELECT ST_Contains(ST_Circle(111-23, 64+23, 10000-90), ST_Point(88, 87))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    print(((1,),) == res)
    ###圆 圆
    line = "SELECT ST_Contains(ST_Circle(111-23, 64+23, 10000-90), ST_Circle(111-23, 64+23, 10000-990))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    ##圆 线 
    line = "SELECT ST_Contains(ST_Circle(88, 87, 100000000000), ST_LineFromText('LINESTRING(88 87, 88 87.000001)'))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    #checkwrong
    line = "SELECT ST_Contains(ST_Circle(111-23, 64+23, 100000000000));"
    runner.checkwrong(line)
    line = "SELECT ST_Contains(ST_Circle(111-23, 64+23, 1000),\
        ST_Contains(ST_Circle(111-23, 64+23, 1000)))"
    runner.checkwrong(line)
    line = "SELECT ST_Contains((111-23, 64+23, 100000000000),(0 0))"
    runner.checkwrong(line)


def test_query_spatial_Distance_Sphere():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_Distance_Sphere",
    "describe": "test for spatial_Distance_Sphere；球面距离;mysql is select st_distance(point(0,0),point(1,1)),其他类型；边界",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_Distance_Sphere；球面距离;mysql is select st_distance(point(0,0),point(1,1))
    其他类型；边界
    """
    #多边形 点；多边形 线
    type_list = ["ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'),\
        ST_LineFromText('LINESTRING(0 0, 0 3)')",\
        "ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))'), \
        ST_Polygon('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))')",\
        "ST_LineFromText('LINESTRING(0 0, 0 3)'), \
        ST_LineFromText('LINESTRING(0 0, 0 3)')",\
        "ST_Circle(111-23, 64+23, 100),ST_Circle(111-23, 64+23, 100)"]
    for index in type_list:
        line = "select st_distance_sphere(%s)" % index
        runner.checkwrong(line)
    ##点 点
    line1 = "select st_distance_sphere(0,0, 0,0)"
    line2 = "select st_distance(point(0,0),point(0,0))"
    runner.check2(line1, line2)
    #其他关键字
    line1 = "select ST_DISTANCE_SPHERE(0,0, 0,0)"
    runner.check2(line1, line2)
    line = "select st_distance_sphere(116.35620117, 39.939093, 116.4274406433, 39.9020987219)"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    print(((7336.913554999592,),) == res)
    line = "select st_distance_sphere(point(0,0),point(0,0))"
    runner.checkwrong(line)


def test_query_spatial_GeometryFromText():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_GeometryFromText",
    "describe": "各个空间类型",
    "tag": "function,p1"
    }
    """
    """
    各个空间类型
    """
    line1 = "SELECT ST_AsText(ST_GeometryFromText('LINESTRING(0 0, 0 10)'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('LINESTRING(0 0, 0 10)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_GeometryFromText('ST_Point(123.12,89.1)'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('Point(123.12,89.1)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_AsText(ST_GeometryFromText('POLYGON ((0 0,0 10,10 10,10 0,\
        11 2,11 0 , -1 -1,0 0))'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('POLYGON ((-1 -1, 11 0, 11 2, 10 0, 10 10,\
        0 10, 0 0, -1 -1))'))"
    runner.check2(line1, line2)
    line1 = " SELECT ST_AsText(ST_GeometryFromText('ST_Circle(111, 64, 10000)'))"
    line2 = "SELECT ST_AsText(GEOMFROMTEXT('Circle(111, 64, 10000)'))"
    runner.check2(line1, line2)


def test_query_spatial_ST_X():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_ST_X",
    "describe": "test for spatial_ST_X；显示X坐标",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_ST_X；显示X坐标
    """
    #多边形 点；多边形 线
    line1 = "SELECT ST_X(ST_GeometryFromText('POLYGON ((0 0,0 10,10 10,10 0,\
       11 2,11 0 , -1 -1,0 0))'))"
    line2 = "SELECT ST_X(GEOMFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_X(ST_Circle(111, 64, 10000))"
    line2 = "SELECT ST_X(GEOMFROMTEXT('Circle(111, 64, 10000)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_X(ST_LINEFROMTEXT('LINESTRING (0 0, 0 10)'))"
    line2 = "SELECT ST_X(GEOMFROMTEXT('LINESTRING (0 0, 0 10)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_X(ST_Point(24.7, 56.7))"
    line2 = "SELECT ST_X(Point(24.7, 56.7))"
    runner.check2(line1, line2)
    lat_list_p = ["(0, 196.7)","(-198, 16.7)","(198, 16.7)","(-18.123456789012345, 16.7)"]
    lat_list_m = ["(NULL, 196.7)","(NULL, 16.7)","(NULL, 16.7)","(-18.123456789012345, 16.7)"]
    #(-123.1234567890123456789, 16.7), (-18.12345678901234, 16.7))
    for p, m in zip(lat_list_p, lat_list_m):
        line1 = "SELECT ST_X(ST_Point%s)" % p
        line2 = "SELECT ST_X(Point%s)" % m
        runner.check2(line1, line2)

    line = "SELECT ST_X(ST_Point(-123456.1234567890123456789, 16.7))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((None,),)
    util.check_same(res, excepted)
    line = "select ST_X(point(0,0),point(0,0))"
    runner.checkwrong(line)
    line = "select ST_X(point(0,0))"
    runner.checkwrong(line)
    line = "select ST_X((-123.1234567890123456789, 16.7))"
    runner.checkwrong(line)


def test_query_spatial_ST_Y():
    """
    {
    "title": "test_query_spatial_function.test_query_spatial_ST_Y",
    "describe": "test for spatial_ST_Y；显示Y坐标, 其他类型；边界",
    "tag": "function,p1,fuzz"
    }
    """
    """
    test for spatial_ST_Y；显示Y坐标
    其他类型；边界
    """
    #多边形 点；多边形 线
    line1 = "SELECT ST_Y(ST_GeometryFromText('POLYGON ((0 0,0 10,10 10,10 0,\
       11 2,11 0 , -1 -1,0 0))'))"
    line2 = "SELECT ST_Y(GEOMFROMTEXT('POLYGON ((0 0,0 10,10 10,10 0, 11 2,11 0 , -1 -1,0 0))'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_Y(ST_Circle(111, 64, 10000))"
    line2 = "SELECT ST_Y(GEOMFROMTEXT('Circle(111, 64, 10000)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_Y(ST_LINEFROMTEXT('LINESTRING (0 0, 0 10)'))"
    line2 = "SELECT ST_Y(GEOMFROMTEXT('LINESTRING (0 0, 0 10)'))"
    runner.check2(line1, line2)
    line1 = "SELECT ST_Y(ST_Point(24.7, 56.7))"
    line2 = "SELECT ST_Y(Point(24.7, 56.7))"
    runner.check2(line1, line2)
    lat_list_p = ["(196.7, 0)", "(16.7, -198)", "(16.7, 198)", "(16.7, -18.123456789012345)"]
    lat_list_m = ["(196.7, NULL)", "(16.7, NULL)", "(16.7, NULL)", "(16.7, -18.123456789012345)"]
    # (-123.1234567890123456789, 16.7), (-18.12345678901234, 16.7))
    for p, m in zip(lat_list_p, lat_list_m):
        line1 = "SELECT ST_Y(ST_Point%s)" % p
        line2 = "SELECT ST_Y(Point%s)" % m
        runner.check2(line1, line2)

    line = "SELECT ST_Y(ST_Point(16.7, -123456.1234567890123456789))"
    LOG.info(L('palo sql', palo_sql=line))
    res = runner.query_palo.do_sql(line)
    print(line, res)
    excepted = ((None,),)
    util.check_same(res, excepted)
    line = "select ST_Y(point(0,0),point(0,0))"
    runner.checkwrong(line)
    line = "select ST_Y(point(0,0))"
    runner.checkwrong(line)
    line = "select ST_Y((16.7, -123.1234567890123456789))"
    runner.checkwrong(line)


if __name__ == "__main__":
    print("test")
    setup_module()
    test_query_spatial_Circle()
