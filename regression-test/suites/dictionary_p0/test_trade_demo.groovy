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

/* groovylint-disable-next-line CompileStatic */
suite('test_trade_demo') {
    sql 'drop database if exists dict_trade_demo'
    sql 'create database dict_trade_demo'
    sql 'use dict_trade_demo'

    sql '''
    -- 广告主信息表
    CREATE TABLE advertiser_info (
        advertiser_id BIGINT NOT NULL COMMENT "广告主ID",
        advertiser_name VARCHAR(128) NOT NULL COMMENT "广告主名称",
        industry_type VARCHAR(64) NOT NULL COMMENT "行业类型",
        account_type VARCHAR(32) NOT NULL COMMENT "账户类型：直客/代理",
        sales_owner VARCHAR(64) NOT NULL COMMENT "销售负责人",
        update_time DATETIME NOT NULL COMMENT "信息更新时间"
    )
    DISTRIBUTED BY HASH(`advertiser_id`) BUCKETS 10
    PROPERTIES("replication_num" = "1");
    '''

    sql '''
    -- 广告创意信息表
    CREATE TABLE creative_info (
        creative_id BIGINT NOT NULL COMMENT "创意ID",
        creative_name VARCHAR(256) NOT NULL COMMENT "创意名称",
        creative_type VARCHAR(32) NOT NULL COMMENT "创意类型：图片/视频/图文",
        material_size VARCHAR(32) NOT NULL COMMENT "素材尺寸",
        landing_page VARCHAR(512) NOT NULL COMMENT "落地页URL",
        update_time DATETIME NOT NULL COMMENT "更新时间"
    )
    DISTRIBUTED BY HASH(`creative_id`) BUCKETS 20
    PROPERTIES("replication_num" = "1");
    '''

    sql '''
    -- 广告投放日志表（事实表）
    CREATE TABLE ad_delivery_logs (
        log_id BIGINT NOT NULL COMMENT "日志ID",
        advertiser_id BIGINT NOT NULL COMMENT "广告主ID",
        creative_id BIGINT NOT NULL COMMENT "创意ID",
        impression BIGINT NOT NULL COMMENT "曝光量",
        click BIGINT NOT NULL COMMENT "点击量",
        cost DECIMAL(20,4) NOT NULL COMMENT "消耗金额",
        conversion INT NOT NULL COMMENT "转化数",
        log_time DATETIME NOT NULL COMMENT "日志时间",
        date_hour VARCHAR NOT NULL COMMENT "小时分区"
    )
    AUTO PARTITION BY LIST(`date_hour`)()
    DISTRIBUTED BY HASH(`log_id`) BUCKETS 32
    PROPERTIES("replication_num" = "1");
    '''

    sql """
    -- 创建广告主信息字典
    CREATE DICTIONARY advertiser_dict USING advertiser_info
    (
        advertiser_id KEY,
        advertiser_name VALUE,
        industry_type VALUE,
        account_type VALUE,
        sales_owner VALUE
    )
    LAYOUT(HASH_MAP)
    PROPERTIES('data_lifetime'='600');  -- 10分钟更新一次
    """

    sql """
    -- 创建创意信息字典
    CREATE DICTIONARY creative_dict USING creative_info
    (
        creative_id KEY,
        creative_name VALUE,
        creative_type VALUE,
        material_size VALUE
    )
    LAYOUT(HASH_MAP)
    PROPERTIES('data_lifetime'='300');  -- 5分钟更新一次
    """

    sql """
    -- 插入广告主信息
    INSERT INTO advertiser_info VALUES
    (10001, '京东商城', '电商', '直客', '张三', '2024-02-20 10:00:00'),
    (10002, '小米科技', '科技', '直客', '李四', '2024-02-20 10:00:00'),
    (10003, '携程旅行', '旅游', '直客', '王五', '2024-02-20 10:00:00'),
    (10004, '蚂蚁金服', '金融', '直客', '赵六', '2024-02-20 10:00:00'),
    (10005, '腾讯游戏', '游戏', '直客', '张三', '2024-02-20 10:00:00'),
    (10006, '美团外卖', '本地生活', '直客', '李四', '2024-02-20 10:00:00'),
    (10007, '网易严选', '电商', '直客', '王五', '2024-02-20 10:00:00'),
    (10008, '字节跳动', '互联网', '直客', '赵六', '2024-02-20 10:00:00'),
    (20001, '某品牌代理公司A', '服装', '代理', '张三', '2024-02-20 10:00:00'),
    (20002, '某品牌代理公司B', '美妆', '代理', '李四', '2024-02-20 10:00:00');

    -- 插入创意信息
    INSERT INTO creative_info VALUES
    (1001, '京东618大促主视觉', '视频', '1242x2208', 'https://promo.jd.com/618', '2024-02-20 10:00:00'),
    (1002, '京东618大促商品图', '图片', '800x800', 'https://promo.jd.com/618/products', '2024-02-20 10:00:00'),
    (1003, '小米14新品发布', '视频', '1920x1080', 'https://www.mi.com/mi14', '2024-02-20 10:00:00'),
    (1004, '小米14产品图', '图片', '1200x1200', 'https://www.mi.com/mi14/buy', '2024-02-20 10:00:00'),
    (1005, '携程特惠机票', '图文', '640x960', 'https://flights.ctrip.com/sale', '2024-02-20 10:00:00'),
    (1006, '携程度假产品', '图片', '800x600', 'https://vacations.ctrip.com', '2024-02-20 10:00:00'),
    (1007, '支付宝红包', '图文', '1242x2208', 'https://ant.alipay.com/redpack', '2024-02-20 10:00:00'),
    (1008, '腾讯游戏预约', '视频', '1920x1080', 'https://game.qq.com/reserve', '2024-02-20 10:00:00'),
    (1009, '美团外卖优惠', '图文', '640x960', 'https://waimai.meituan.com', '2024-02-20 10:00:00'),
    (1010, '网易严选新品', '图片', '800x800', 'https://you.163.com/new', '2024-02-20 10:00:00');

    -- 插入广告投放日志（模拟2024-02-23的16-17点数据）
    INSERT INTO ad_delivery_logs VALUES
    -- 京东商城的投放数据
    (1, 10001, 1001, 50000, 2500, 5000.0000, 100, '2024-02-23 16:00:00', '2024022316'),
    (2, 10001, 1002, 45000, 2250, 4500.0000, 90, '2024-02-23 16:00:00', '2024022316'),
    (3, 10001, 1001, 48000, 2400, 4800.0000, 96, '2024-02-23 17:00:00', '2024022317'),
    (4, 10001, 1002, 46000, 2300, 4600.0000, 92, '2024-02-23 17:00:00', '2024022317'),

    -- 小米科技的投放数据
    (5, 10002, 1003, 60000, 3600, 7200.0000, 180, '2024-02-23 16:00:00', '2024022316'),
    (6, 10002, 1004, 55000, 3300, 6600.0000, 165, '2024-02-23 16:00:00', '2024022316'),
    (7, 10002, 1003, 58000, 3480, 6960.0000, 174, '2024-02-23 17:00:00', '2024022317'),
    (8, 10002, 1004, 56000, 3360, 6720.0000, 168, '2024-02-23 17:00:00', '2024022317'),

    -- 携程旅行的投放数据
    (9, 10003, 1005, 40000, 1600, 3200.0000, 48, '2024-02-23 16:00:00', '2024022316'),
    (10, 10003, 1006, 38000, 1520, 3040.0000, 46, '2024-02-23 16:00:00', '2024022316'),
    (11, 10003, 1005, 42000, 1680, 3360.0000, 50, '2024-02-23 17:00:00', '2024022317'),
    (12, 10003, 1006, 39000, 1560, 3120.0000, 47, '2024-02-23 17:00:00', '2024022317'),

    -- 蚂蚁金服的投放数据
    (13, 10004, 1007, 70000, 4200, 8400.0000, 210, '2024-02-23 16:00:00', '2024022316'),
    (14, 10004, 1007, 68000, 4080, 8160.0000, 204, '2024-02-23 17:00:00', '2024022317'),

    -- 腾讯游戏的投放数据
    (15, 10005, 1008, 65000, 5200, 10400.0000, 260, '2024-02-23 16:00:00', '2024022316'),
    (16, 10005, 1008, 63000, 5040, 10080.0000, 252, '2024-02-23 17:00:00', '2024022317'),

    -- 美团外卖的投放数据
    (17, 10006, 1009, 55000, 2750, 5500.0000, 138, '2024-02-23 16:00:00', '2024022316'),
    (18, 10006, 1009, 53000, 2650, 5300.0000, 133, '2024-02-23 17:00:00', '2024022317'),

    -- 网易严选的投放数据
    (19, 10007, 1010, 35000, 1400, 2800.0000, 42, '2024-02-23 16:00:00', '2024022316'),
    (20, 10007, 1010, 36000, 1440, 2880.0000, 43, '2024-02-23 17:00:00', '2024022317'),

    -- 字节跳动的投放数据
    (21, 10008, 1001, 75000, 6000, 12000.0000, 300, '2024-02-23 16:00:00', '2024022316'),
    (22, 10008, 1001, 73000, 5840, 11680.0000, 292, '2024-02-23 17:00:00', '2024022317'),

    -- 代理公司A的投放数据
    (23, 20001, 1002, 25000, 1000, 2000.0000, 30, '2024-02-23 16:00:00', '2024022316'),
    (24, 20001, 1002, 26000, 1040, 2080.0000, 31, '2024-02-23 17:00:00', '2024022317'),

    -- 代理公司B的投放数据
    (25, 20002, 1003, 28000, 1120, 2240.0000, 34, '2024-02-23 16:00:00', '2024022316'),
    (26, 20002, 1003, 27000, 1080, 2160.0000, 32, '2024-02-23 17:00:00', '2024022317');

    -- 为了测试全天数据的查询，添加0-23点的数据（这里只展示部分插入语句）
    INSERT INTO ad_delivery_logs VALUES
    (27, 10001, 1001, 30000, 1500, 3000.0000, 60, '2024-02-23 00:00:00', '2024022300'),
    (28, 10002, 1003, 35000, 2100, 4200.0000, 105, '2024-02-23 01:00:00', '2024022301'),
    (29, 10003, 1005, 25000, 1000, 2000.0000, 30, '2024-02-23 02:00:00', '2024022302'),
    (30, 10004, 1007, 40000, 2400, 4800.0000, 120, '2024-02-23 03:00:00', '2024022303');
    """

    sql "sync"
    sql 'refresh dictionary advertiser_dict'
    sql 'refresh dictionary creative_dict'

    order_qt_sql1 '''
    SELECT 
        l.date_hour,
        a.advertiser_name,
        a.industry_type,
        a.account_type,
        a.sales_owner,
        SUM(l.impression) as total_impression,
        SUM(l.click) as total_click,
        SUM(l.cost) as total_cost,
        SUM(l.conversion) as total_conversion,
        ROUND(SUM(l.click) * 100.0 / NULLIF(SUM(l.impression), 0), 2) as ctr,
        ROUND(SUM(l.cost) * 1000.0 / NULLIF(SUM(l.impression), 0), 2) as cpm,
        ROUND(SUM(l.cost) / NULLIF(SUM(l.click), 0), 2) as cpc,
        ROUND(SUM(l.conversion) * 100.0 / NULLIF(SUM(l.click), 0), 2) as cvr
    FROM ad_delivery_logs l
    LEFT JOIN advertiser_info a ON l.advertiser_id = a.advertiser_id
    WHERE l.date_hour >= '2024022316'
        AND l.date_hour <= '2024022317'
    GROUP BY 
        l.date_hour,
        a.advertiser_name,
        a.industry_type,
        a.account_type,
        a.sales_owner
    ORDER BY total_cost DESC
    LIMIT 100;
    '''

    order_qt_sql1_dict '''
    WITH advertiser_metrics AS (
        SELECT 
            l.date_hour,
            l.advertiser_id,
            dict_get_many("dict_trade_demo.advertiser_dict", 
                ["advertiser_name", "industry_type", "account_type", "sales_owner"], 
                struct(l.advertiser_id)
            ) as advertiser_info,
            SUM(l.impression) as total_impression,
            SUM(l.click) as total_click,
            SUM(l.cost) as total_cost,
            SUM(l.conversion) as total_conversion
        FROM ad_delivery_logs l
        WHERE l.date_hour >= '2024022316'
            AND l.date_hour <= '2024022317'
        GROUP BY l.date_hour, l.advertiser_id
    )
    SELECT 
        date_hour,
        struct_element(advertiser_info, 'advertiser_name'),
        struct_element(advertiser_info, 'industry_type'),
        struct_element(advertiser_info, 'account_type'),
        struct_element(advertiser_info, 'sales_owner'),
        total_impression,
        total_click,
        total_cost,
        total_conversion,
        ROUND(total_click * 100.0 / NULLIF(total_impression, 0), 2) as ctr,
        ROUND(total_cost * 1000.0 / NULLIF(total_impression, 0), 2) as cpm,
        ROUND(total_cost / NULLIF(total_click, 0), 2) as cpc,
        ROUND(total_conversion * 100.0 / NULLIF(total_click, 0), 2) as cvr
    FROM advertiser_metrics
    ORDER BY total_cost DESC
    LIMIT 100;
    '''

    order_qt_sql2 '''
    SELECT 
        c.creative_type,
        c.material_size,
        a.industry_type,
        COUNT(DISTINCT l.creative_id) as creative_count,
        SUM(l.impression) as total_impression,
        SUM(l.click) as total_click,
        ROUND(SUM(l.click) * 100.0 / NULLIF(SUM(l.impression), 0), 2) as ctr,
        SUM(l.cost) as total_cost,
        ROUND(SUM(l.cost) * 1000.0 / NULLIF(SUM(l.impression), 0), 2) as cpm
    FROM ad_delivery_logs l
    LEFT JOIN creative_info c ON l.creative_id = c.creative_id
    LEFT JOIN advertiser_info a ON l.advertiser_id = a.advertiser_id
    WHERE l.date_hour >= '2024022300'
        AND l.date_hour <= '2024022323'
    GROUP BY 
        c.creative_type,
        c.material_size,
        a.industry_type
    HAVING total_impression > 10000
    ORDER BY total_cost DESC;
    '''

    order_qt_sql2_dict '''
    WITH creative_metrics AS (
        SELECT 
            l.creative_id,
            l.advertiser_id,
            dict_get_many("dict_trade_demo.creative_dict", 
                ["creative_type", "material_size"], 
                struct(l.creative_id)
            ) as creative_info,
            dict_get("dict_trade_demo.advertiser_dict", "industry_type", l.advertiser_id) as industry_type,
            SUM(l.impression) as total_impression,
            SUM(l.click) as total_click,
            SUM(l.cost) as total_cost
        FROM ad_delivery_logs l
        WHERE l.date_hour >= '2024022300'
            AND l.date_hour <= '2024022323'
        GROUP BY l.creative_id, l.advertiser_id
    )
    SELECT 
        struct_element(creative_info, 'creative_type'),
        struct_element(creative_info, 'material_size'),
        industry_type,
        COUNT(DISTINCT creative_id) as creative_count,
        SUM(total_impression) as total_impression,
        SUM(total_click) as total_click,
        ROUND(SUM(total_click) * 100.0 / NULLIF(SUM(total_impression), 0), 2) as ctr,
        SUM(total_cost) as total_cost,
        ROUND(SUM(total_cost) * 1000.0 / NULLIF(SUM(total_impression), 0), 2) as cpm
    FROM creative_metrics
    GROUP BY 
        struct_element(creative_info, 'creative_type'),
        struct_element(creative_info, 'material_size'),
        industry_type
    HAVING total_impression > 10000
    ORDER BY total_cost DESC;
    '''
}
