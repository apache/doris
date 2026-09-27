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

// Source: query string and search+join examples, revision 23.
suite("test_crm_search_join_document") {
    sql "set enable_match_without_inverted_index = false"
    // The pipeline randomizes these defaults. A VARIANT subcolumn stored in the sparse or doc column has no
    // inverted index, so SEARCH finds nothing in it and MATCH fails without enable_match_without_inverted_index.
    sql "set default_variant_enable_doc_mode = false"
    sql "set default_variant_enable_typed_paths_to_sparse = false"
    sql "set default_variant_max_subcolumns_count = 0"

    sql "DROP TABLE IF EXISTS crm_search_objects"
    sql """CREATE TABLE crm_search_objects (
        OBJECTID BIGINT, PORTALID BIGINT, OBJECTTYPEID STRING, OBJECTIDHASH INT,
        DELETED BOOLEAN, INGESTIONTIMESTAMP BIGINT, PROCESSEDTIMESTAMP BIGINT,
        VERSION BIGINT, OVERFLOWPROPERTIES VARIANT,
        INDEX idx_properties(OVERFLOWPROPERTIES) USING INVERTED PROPERTIES("parser"="english")
    ) DUPLICATE KEY(OBJECTID) DISTRIBUTED BY HASH(OBJECTID) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_objects VALUES
        (-1,865815822,'0-1',0,false,1,1,1,parse_to_variant('{"string_0":"nobody","string_8":"nobody","string_9":"nobody","string_17":"nobody","number_array_1":[1,2]}')),
        (0,865815822,'0-1',0,false,2,2,1,parse_to_variant('{"string_0":"nobody","string_8":"nobody","string_9":"nobody","string_17":"nobody"}')),
        (1,865815822,'0-1',0,false,3,3,1,parse_to_variant('{"string_0":"john","string_8":"john jane keyur patel hr","string_9":"jane","string_17":"john patel","number_array_1":[1,2]}')),
        (2,865815822,'0-1',0,false,4,4,1,parse_to_variant('{"string_0":"nobody","string_8":"john","string_9":"jane","string_17":"patel","number_array_1":[3]}')),
        (3,865815822,'0-1',0,false,5,5,1,parse_to_variant('{"string_0":"john","string_8":"john","string_17":"john"}')),
        (4,865815822,'0-1',0,false,6,6,1,parse_to_variant('{}')),
        (5,865815822,'0-1',0,false,7,7,1,NULL),
        (6,865815822,'0-1',0,true,8,8,1,parse_to_variant('{"string_8":"john"}')),
        (7,7,'0-1',0,false,9,9,1,parse_to_variant('{"string_8":"john"}')),
        (101,865815822,'0-3',0,false,10,10,1,parse_to_variant('{"string_3":"naive"}')),
        (102,865815822,'0-3',0,false,11,11,1,parse_to_variant('{"string_3":"other"}')),
        (103,865815822,'0-3',0,false,12,12,1,parse_to_variant('{"string_3":"naive"}')),
        (201,865815822,'0-2',0,false,13,13,1,parse_to_variant('{}'))"""
    sql "DROP TABLE IF EXISTS crm_search_lists"
    sql """CREATE TABLE crm_search_lists (
        OBJECTID BIGINT, PORTALID BIGINT, OBJECTTYPEID STRING,
        OBJECTIDHASH INT, LISTID BIGINT, DELETED BOOLEAN
    ) DUPLICATE KEY(OBJECTID) DISTRIBUTED BY HASH(OBJECTID) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_lists VALUES
        (-1,865815822,'0-1',0,12,false),(0,865815822,'0-1',0,12,false),
        (1,865815822,'0-1',0,456,false),(1,865815822,'0-1',0,123,false),
        (1,865815822,'0-1',0,12,false),(2,865815822,'0-1',0,123,false),
        (2,865815822,'0-1',0,12,false),(4,865815822,'0-1',0,12,false),
        (5,865815822,'0-1',0,12,false),(99,865815822,'0-1',0,12,false),
        (1,1,'0-1',0,12,false),(2,1,'0-1',1,12,false),
        (NULL,1,'0-1',0,12,false)"""
    sql "DROP TABLE IF EXISTS crm_search_associations"
    sql """CREATE TABLE crm_search_associations (
        FROMOBJECTID BIGINT, TOOBJECTID BIGINT, PORTALID BIGINT,
        FROMOBJECTTYPEID STRING, FROMOBJECTIDHASH INT,
        COMBINEDASSOCIATIONTYPEID STRING, DELETED BOOLEAN
    ) DUPLICATE KEY(FROMOBJECTID) DISTRIBUTED BY HASH(FROMOBJECTID) BUCKETS 1
    PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_associations VALUES
        (201,101,865815822,'0-2',0,'0-342',false),
        (1,102,865815822,'0-1',0,'0-4',false),
        (1,1,865815822,'0-1',0,'0-123',false),
        (2,1,865815822,'0-1',0,'0-123',false),
        (1,10,1,'0-1',0,'0-1',false),
        (2,11,1,'0-1',2,'0-1',false),
        (NULL,12,1,'0-1',0,'0-1',false)"""

    // Document example 0: 一、多列IN语法报错SQL（初始问题：多字段元组IN子查询不支持） / 报错原始SQL
    test {
        sql """WITH lists AS (
  SELECT
    portalId,
    objectTypeId,
    objectId,
    objectIdHash
  FROM
    crm_search_lists
  where
    portalId = 1
    and objectTypeId = '0-1'
    and objectIdHash IN (0, 1, 2)
),
associations AS (
  SELECT
    portalId,
    fromObjectTypeId,
    fromObjectIdHash,
    fromObjectId,
    toObjectId
  FROM
    crm_search_associations
  where
    portalId = 1
    and combinedAssociationTypeId = '0-1'
    and fromObjectIdHash IN (0, 1, 2)
)
SELECT
  objectId
FROM
  lists
WHERE
  (portalId, objectTypeId, objectIdHash, objectId) IN (
    SELECT
      portalId,
      fromObjectTypeId,
      fromObjectIdHash,
      fromObjectId
    FROM
      associations
  )"""
        exception "mismatched input ','"
    }

    // Document example 1: 一、多列IN语法报错SQL（初始问题：多字段元组IN子查询不支持） / 官方替代方案：EXISTS改写（无语法报错）
    order_qt_document_01 """
WITH lists AS (
  SELECT
    portalId,
    objectTypeId,
    objectId,
    objectIdHash
  FROM
    crm_search_lists
  WHERE
    portalId = 1
    AND objectTypeId = '0-1'
    AND objectIdHash IN (0, 1, 2)
),
associations AS (
  SELECT
    portalId,
    fromObjectTypeId,
    fromObjectIdHash,
    fromObjectId,
    toObjectId
  FROM
    crm_search_associations
  WHERE
    portalId = 1
    AND combinedAssociationTypeId = '0-1'
    AND fromObjectIdHash IN (0, 1, 2)
)
SELECT
  l.objectId
FROM
  lists l
WHERE
  EXISTS (
    SELECT 1
    FROM associations a
    WHERE
      a.portalId = l.portalId
      AND a.fromObjectTypeId = l.objectTypeId
      AND a.fromObjectIdHash = l.objectIdHash
      AND a.fromObjectId = l.objectId
  )
    """

    // Document example 2: 二、OR+EXISTS触发Mark Join报错（SlotReference异常） / 2.1 正常可执行SQL（无OR，无报错）
    order_qt_document_02 """
WITH contacts AS (
  select
    OBJECTID,
    INGESTIONTIMESTAMP,
    cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as firstname
  from
    crm_search_objects
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and OBJECTIDHASH IN (0)
      and DELETED = false
    )
),
lists AS (
  select
    PORTALID as PORTALID,
    LISTID as LISTID,
    OBJECTID as OBJECTID
  from
    crm_search_lists
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and DELETED = false
    )
),
results AS (
  select
    contacts.OBJECTID,
    contacts.INGESTIONTIMESTAMP,
    contacts.firstname
  from
    contacts
  where
    (
      (firstname MATCH_ANY 'keyur patel')
      and EXISTS (
        SELECT
          1
        FROM
          lists l
        WHERE
          l.OBJECTID = contacts.OBJECTID
      )
    )
)
SELECT
  *
FROM
  results
LIMIT
  10
    """

    // Document example 3: 二、OR+EXISTS触发Mark Join报错（SlotReference异常） / 2.2 报错SQL（新增OR触发Mark Join，报SlotReference缺失列）
    order_qt_document_03 """
WITH contacts AS (
  select
    OBJECTID,
    INGESTIONTIMESTAMP,
    cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as firstname
  from
    crm_search_objects
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and OBJECTIDHASH IN (0)
      and DELETED = false
    )
),
lists AS (
  select
    PORTALID as PORTALID,
    LISTID as LISTID,
    OBJECTID as OBJECTID
  from
    crm_search_lists
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and DELETED = false
    )
),
results AS (
  select
    contacts.OBJECTID,
    contacts.INGESTIONTIMESTAMP,
    contacts.firstname
  from
    contacts
  where
    (
      (firstname MATCH_ANY 'keyur patel')
      and EXISTS (
        SELECT
          1
        FROM
          lists l
        WHERE
          l.OBJECTID = contacts.OBJECTID
      )
    )
    OR contacts.OBJECTID > 1
)
SELECT
  *
FROM
  results
LIMIT
  10
    """

    // Document example 4: 二、OR+EXISTS触发Mark Join报错（SlotReference异常） / 2.3 临时规避写法（MATCH过滤下推至内层CTE，避免外层OR关联）
    order_qt_document_04 """
WITH contacts AS (
  SELECT
    OBJECTID,
    INGESTIONTIMESTAMP,
    CAST(OVERFLOWPROPERTIES['string_8'] AS VARCHAR) AS firstname
  FROM
    crm_search_objects
  WHERE
    PORTALID = 865815822
    AND OBJECTTYPEID = '0-1'
    AND OBJECTIDHASH IN (0)
    AND DELETED = FALSE
    -- MATCH过滤下推至内层，规避外层JOIN OR逻辑
    AND CAST(OVERFLOWPROPERTIES['string_8'] AS VARCHAR) MATCH_ANY 'keyur patel'
),
lists AS (
  SELECT
    PORTALID,
    LISTID,
    OBJECTID
  FROM
    crm_search_lists
  WHERE
    PORTALID = 865815822
    AND OBJECTTYPEID = '0-1'
    AND DELETED = FALSE
),
results AS (
  SELECT
    c.OBJECTID,
    c.INGESTIONTIMESTAMP,
    c.firstname
  FROM
    contacts c
  WHERE
    EXISTS (
      SELECT 1
      FROM lists l
      WHERE l.OBJECTID = c.OBJECTID
    )
    OR c.OBJECTID > 1
)
SELECT *
FROM results
LIMIT 10
    """

    // Document example 5: 三、SEARCH函数JOIN限制报错SQL（search不能出现在多表关联后过滤） / 报错SQL（含多表LEFT JOIN + SEARCH函数，FE直接拦截）
    order_qt_document_05 """
select
  objects_base.OBJECTID,
  cast(
    objects_base.OVERFLOWPROPERTIES ['string_17'] as VARCHAR
  )
from
  (
    select
      PORTALID,
      OBJECTTYPEID,
      OBJECTID,
      DELETED,
      INGESTIONTIMESTAMP,
      PROCESSEDTIMESTAMP,
      VERSION,
      OVERFLOWPROPERTIES
    from
      crm_search_objects
    where
      (
        PORTALID = 865815822
        and OBJECTTYPEID = '0-1'
        and OBJECTIDHASH IN (0)
        and DELETED = false
      )
  ) as objects_base
  left outer join (
    select
      *
    from
      crm_search_lists
    where
      (
        PORTALID = 865815822
        and OBJECTTYPEID = '0-1'
        and DELETED = false
      )
  ) as lists_base on objects_base.OBJECTID = lists_base.OBJECTID
  left outer join (
    select
      *
    from
      crm_search_associations
    where
      (
        PORTALID = 865815822
        and DELETED = false
        and COMBINEDASSOCIATIONTYPEID in ('0-123')
      )
  ) as associations_base on objects_base.OBJECTID = associations_base.TOOBJECTID
where
  search(
    "john",
    '{"default_field":"OVERFLOWPROPERTIES.string_17","mode":"lucene"}'
  )
    """

    // Document example 6: 四、PR#60839性能退化测试样例（多层CTE+多OR+MATCH） /
    order_qt_document_06 """
with objects_0_3 as (
  select
    OBJECTID as _OBJECTID,
    INGESTIONTIMESTAMP as _INGESTIONTIMESTAMP,
    cast(OVERFLOWPROPERTIES ['string_3'] as VARCHAR) as dealname
  from
    crm_search_objects
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-3'
      and OBJECTIDHASH IN (0)
      and DELETED = false
    )
),
objects_xo_0_0_2 as (
  select
    OBJECTID as _OBJECTID,
    INGESTIONTIMESTAMP as _INGESTIONTIMESTAMP
  from
    crm_search_objects
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-2'
      and OBJECTIDHASH IN (0)
      and DELETED = false
    )
),
objects_xo_1_0_1 as (
  select
    OBJECTID as _OBJECTID,
    INGESTIONTIMESTAMP as _INGESTIONTIMESTAMP
  from
    crm_search_objects
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and OBJECTIDHASH IN (0)
      and DELETED = false
    )
),
xo_assoc_0_0_342 as (
  select
    FROMOBJECTID as FROMOBJECTID,
    TOOBJECTID as TOOBJECTID
  from
    crm_search_associations
  where
    (
      PORTALID = 865815822
      and DELETED = false
      and COMBINEDASSOCIATIONTYPEID in ('0-342')
    )
),
xo_assoc_1_0_4 as (
  select
    FROMOBJECTID as FROMOBJECTID,
    TOOBJECTID as TOOBJECTID
  from
    crm_search_associations
  where
    (
      PORTALID = 865815822
      and DELETED = false
      and COMBINEDASSOCIATIONTYPEID in ('0-4')
    )
),
secondary_0_0_2 as (
  select
    objects_xo_0_0_2._OBJECTID as _OBJECTID
  from
    objects_xo_0_0_2
),
xo_result_0 as (
  select
    distinct a.TOOBJECTID as _OBJECTID
  from
    xo_assoc_0_0_342 as a
    join secondary_0_0_2 as s on a.FROMOBJECTID = s._OBJECTID
),
secondary_1_0_1 as (
  select
    objects_xo_1_0_1._OBJECTID as _OBJECTID
  from
    objects_xo_1_0_1
),
xo_result_1 as (
  select
    distinct a.TOOBJECTID as _OBJECTID
  from
    xo_assoc_1_0_4 as a
    join secondary_1_0_1 as s on a.FROMOBJECTID = s._OBJECTID
),
results as (
  select
    objects_0_3._OBJECTID as _OBJECTID
  from
    objects_0_3
    left join xo_result_0 xo0 on objects_0_3._OBJECTID = xo0._OBJECTID
    left join xo_result_1 xo1 on objects_0_3._OBJECTID = xo1._OBJECTID
  where
    (
      (objects_0_3.dealname MATCH_ALL 'naive')
      and xo0._OBJECTID is not null
    )
    or (xo1._OBJECTID is not null)
)
select
  *
from
  results
    """

    // Document example 7: 五、Variant列JOIN+MATCH混合查询（验证子列裁剪） /
    order_qt_document_07 """
select
  `contacts`.OBJECTID,
  cast(
    `contacts`.OVERFLOWPROPERTIES ['string_8'] as VARCHAR
  ),
  cast(
    `contacts`.OVERFLOWPROPERTIES ['number_array_1'] as ARRAY<DOUBLE>
  )
from
  (
    select
      OBJECTID,
      OBJECTIDHASH,
      OVERFLOWPROPERTIES
    from
      crm_search_objects
    where
      (
        PORTALID = 865815822
        and OBJECTTYPEID = '0-1'
        and OBJECTIDHASH IN (0)
        and DELETED = false
      )
  ) as `contacts`
  left outer join (
    select
      OBJECTID
    from
      crm_search_lists
    where
      (
        PORTALID = 865815822
        and OBJECTTYPEID = '0-1'
        and DELETED = false
      )
  ) as `contact_lists` on `contacts`.OBJECTID = `contact_lists`.OBJECTID
where
  (
    (
      cast(
        `contacts`.OVERFLOWPROPERTIES ['string_8'] as VARCHAR
      ) match_all 'john'
    )
    and contact_lists.OBJECTID > 0
  )
  OR contact_lists.OBJECTID is not null
    """

    // Document example 8: 六、OR多条件JOIN执行计划样例SQL（可下推部分过滤） /
    // The source references agg_lists without defining it; supply distinct member IDs.
    order_qt_document_08 """
WITH agg_lists AS (SELECT DISTINCT OBJECTID FROM crm_search_lists)
select
    obj.OBJECTID as OBJECTID,
    cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as objects_0_1__firstname
  from
    crm_search_objects as obj
    left outer join agg_lists on obj.OBJECTID = agg_lists.OBJECTID
  where
    PORTALID = 865815822
    and OBJECTTYPEID = '0-1'
    and OBJECTIDHASH IN (0)
    and DELETED = false
    AND (
      (
        (
          cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) MATCH_ALL 'john'
        )
        AND agg_lists.OBJECTID = 2
      )
      OR (
        agg_lists.OBJECTID = 1
        AND cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) MATCH_ALL 'jane'
      )
    )
    """

    // Document example 9: 七、PR#61092两种写法对比（是否预提取MATCH为虚拟列） / 7.1 原始写法（MATCH写在WHERE，跨JOIN OR无法下推索引）
    order_qt_document_09 """
WITH objects AS (
SELECT
  objectId,
  CAST(overflowProperties['string_0'] AS VARCHAR) firstName
FROM crm_search_objects
),
lists AS (
   SELECT objectId FROM crm_search_lists WHERE listId = 12
)
SELECT
    o.objectId
FROM objects o LEFT JOIN lists l ON o.objectId = l.objectId
WHERE firstName MATCH_ANY 'john' OR l.objectId IS NOT NULL
    """

    // Document example 10: 七、PR#61092两种写法对比（是否预提取MATCH为虚拟列） / 7.2 优化写法（内层CTE预计算MATCH布尔虚拟列，触发索引fast path）
    // Move the source misplaced alias t1 onto the inner derived table.
    order_qt_document_10 """
WITH objects AS (
   SELECT objectId, firstName, firstName MATCH_ANY 'john' AS firstNameFilter1
   FROM (
      SELECT
        objectId,
        CAST(overflowProperties['string_0'] AS VARCHAR) firstName
      FROM crm_search_objects
    ) t1
),
lists AS (
   SELECT objectId FROM crm_search_lists WHERE listId = 12
)
SELECT
    o.objectId
FROM objects o LEFT JOIN lists l ON o.objectId = l.objectId
WHERE firstNameFilter1 OR l.objectId IS NOT NULL
    """

    // Document example 11: 八、多层CTE复杂OR场景（两种版本，区分能否下推过滤） / 8.1 无法下推过滤版本
    order_qt_document_11 """
with lists as (
  select
    PORTALID as PORTALID,
    LISTID as LISTID,
    OBJECTID as OBJECTID
  from
    crm_search_lists
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and DELETED = false
    )
),
agg_lists as (
  select
    OBJECTID,
    array_agg(LISTID) as listIds
  from
    lists
  group by
    OBJECTID
),
fg_1 as (
  select
    obj.OBJECTID as OBJECTID,
    cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as objects_0_1__firstname
  from
    crm_search_objects as obj
  where
    PORTALID = 865815822
    and OBJECTTYPEID = '0-1'
    and OBJECTIDHASH IN (0)
    and DELETED = false
    AND (
      (
        (
          cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) MATCH_ALL 'john'
        )
        OR (
          cast(OVERFLOWPROPERTIES ['string_9'] as VARCHAR) MATCH_ALL 'jane'
        )
        OR EXISTS (
          SELECT
            1
          FROM
            agg_lists l
          WHERE
            l.OBJECTID = obj.OBJECTID
        )
      )
    )
)
select
  OBJECTID
from
  fg_1
    """

    // Document example 12: 八、多层CTE复杂OR场景（两种版本，区分能否下推过滤） / 8.2 可下推过滤版本（新增同表OBJECTID>0约束拆分OR）
    order_qt_document_12 """
with lists as (
  select
    PORTALID as PORTALID,
    LISTID as LISTID,
    OBJECTID as OBJECTID
  from
    crm_search_lists
  where
    (
      PORTALID = 865815822
      and OBJECTTYPEID = '0-1'
      and DELETED = false
    )
),
agg_lists as (
  select
    OBJECTID,
    array_agg(LISTID) as listIds
  from
    lists
  group by
    OBJECTID
),
fg_1 as (
  select
    obj.OBJECTID as OBJECTID,
    cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as objects_0_1__firstname
  from
    crm_search_objects as obj
  where
    PORTALID = 865815822
    and OBJECTTYPEID = '0-1'
    and OBJECTIDHASH IN (0)
    and DELETED = false
    AND (
      (
        cast(OVERFLOWPROPERTIES ['string_8'] as VARCHAR) MATCH_ALL 'john'
      )
      OR (
        cast(OVERFLOWPROPERTIES ['string_9'] as VARCHAR) MATCH_ALL 'jane'
      )
      OR (
        OBJECTID > 0
        and EXISTS (
          SELECT
            1
          FROM
            agg_lists l
          WHERE
            l.OBJECTID = obj.OBJECTID
        )
      )
    )
)
select
  OBJECTID
from
  fg_1
    """

    // Document example 13: 九、多条件AND+OR嵌套JOIN查询（带array_intersect数组过滤） /
    // The source final SELECT uses a missing OBJECTID alias; select the declared alias.
    order_qt_document_13 """
with `agg_lists` as (
  select
    OBJECTID,
    OBJECTTYPEID,
    OBJECTIDHASH,
    array_agg(LISTID) as `listIds`
  from
    (
      select
        PORTALID,
        LISTID,
        OBJECTID,
        OBJECTTYPEID,
        OBJECTIDHASH
      from
        crm_search_lists
      where
        (
          PORTALID = 865815822
          and OBJECTTYPEID = '0-1'
          and DELETED = false
          and LISTID IN (456,123)
          and OBJECTIDHASH IN (0)
        )
    ) as `lists`
  group by
    OBJECTID,
    OBJECTTYPEID,
    OBJECTIDHASH
),
`results` as (
  select
    `objects_0_1`.`OBJECTID` as `objects_0_1___OBJECTID`,
    `objects_0_1`.`INGESTIONTIMESTAMP` as `objects_0_1___INGESTIONTIMESTAMP`,
    `objects_0_1`.`OBJECTTYPEID` as `objects_0_1___OBJECTTYPEID`,
    `objects_0_1`.`OBJECTIDHASH` as `objects_0_1___OBJECTIDHASH`,
    cast(`objects_0_1`.OVERFLOWPROPERTIES ['string_8'] as VARCHAR) as `objects_0_1__firstname`,
    cast(
      `objects_0_1`.OVERFLOWPROPERTIES ['string_17'] as VARCHAR
    ) as `objects_0_1__lastname`,
    `agg_lists`.`OBJECTID` as `agg_lists__OBJECTID`,
    `agg_lists`.`OBJECTTYPEID` as `agg_lists__OBJECTTYPEID`,
    `agg_lists`.`OBJECTIDHASH` as `agg_lists__OBJECTIDHASH`,
    `agg_lists`.`listIds` as `agg_lists__listIds`
  from
    (
      select
        OBJECTID,
        INGESTIONTIMESTAMP,
        OBJECTTYPEID,
        OBJECTIDHASH,
        OVERFLOWPROPERTIES
      from
        crm_search_objects
      where
        (
          PORTALID = 865815822
          and OBJECTTYPEID = '0-1'
          and OBJECTIDHASH IN (0)
          and DELETED = false
        )
    ) as `objects_0_1`
    left outer join `agg_lists` on (
      (
        `objects_0_1`.`OBJECTID` = `agg_lists`.`OBJECTID`
      )
      and (
        `objects_0_1`.`OBJECTTYPEID` = `agg_lists`.`OBJECTTYPEID`
      )
      and (
        `objects_0_1`.`OBJECTIDHASH` = `agg_lists`.`OBJECTIDHASH`
      )
    )
  where
    (
      (
        (
          cast(`objects_0_1`.OVERFLOWPROPERTIES ['string_8'] as VARCHAR) MATCH_ALL 'hr'
        )
        and (
          array_size(
            array_intersect(`agg_lists`.`listIds`, array(456))
          ) = 1
        )
      )
      or (
        (
          array_size(
            array_intersect(`agg_lists`.`listIds`, array(123))
          ) = 1
        )
        and (
          cast(
            `objects_0_1`.OVERFLOWPROPERTIES ['string_17'] as VARCHAR
          ) MATCH_ALL 'patel'
        )
      )
    )
)
select objects_0_1___OBJECTID from `results`
    """

    // Chapter XIV: these two predicates are intentionally different.
    sql "DROP TABLE IF EXISTS crm_search_full_a"
    sql """CREATE TABLE crm_search_full_a (k1 INT, content TEXT,
        INDEX idx_content(content) USING INVERTED PROPERTIES("parser"="english"))
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "DROP TABLE IF EXISTS crm_search_full_b"
    sql """CREATE TABLE crm_search_full_b (k1 INT)
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql """INSERT INTO crm_search_full_a VALUES
        (1,'hello world'),(2,'hello'),(3,'world'),(4,'neither'),
        (5,NULL),(6,'hello'),(7,'world'),(NULL,'hello world')"""
    sql "INSERT INTO crm_search_full_b VALUES (1),(1),(4),(5),(6),(8),(NULL)"
    order_qt_document_14_original """
        SELECT a.k1, b.k1 FROM crm_search_full_a a FULL OUTER JOIN crm_search_full_b b
        ON a.k1=b.k1 WHERE b.k1>2
        OR (a.content MATCH_ALL 'hello' AND a.content MATCH_ALL 'world')
        OR (a.content MATCH_ALL 'hello' AND b.k1>5)
    """
    order_qt_document_14_alternative """
        SELECT a.k1, b.k1 FROM crm_search_full_a a FULL OUTER JOIN crm_search_full_b b
        ON a.k1=b.k1 WHERE (b.k1>2 AND b.k1 IS NOT NULL)
        OR a.content MATCH_ALL 'hello' OR a.content MATCH_ALL 'world'
    """

    // SEARCH must not remove rows selected solely by the association branch.
    order_qt_search_join_or """
        SELECT o.OBJECTID, l.LISTID FROM crm_search_objects o
        LEFT JOIN crm_search_lists l ON o.OBJECTID=l.OBJECTID
        WHERE o.PORTALID=865815822 AND o.OBJECTTYPEID='0-1' AND NOT o.DELETED
        AND (search('OVERFLOWPROPERTIES.string_8:john') OR l.LISTID=12)
    """
    order_qt_search_two_joins_or """
        SELECT o.OBJECTID, l.LISTID, a.FROMOBJECTID FROM crm_search_objects o
        LEFT JOIN crm_search_lists l ON o.OBJECTID=l.OBJECTID
        LEFT JOIN crm_search_associations a ON o.OBJECTID=a.TOOBJECTID
        WHERE o.PORTALID=865815822 AND o.OBJECTTYPEID='0-1' AND NOT o.DELETED
        AND ((search('OVERFLOWPROPERTIES.string_8:john') AND l.LISTID=456)
             OR a.FROMOBJECTID=2 OR l.LISTID=12)
    """
    order_qt_search_join_not """
        SELECT o.OBJECTID, l.LISTID FROM crm_search_objects o
        LEFT JOIN crm_search_lists l ON o.OBJECTID=l.OBJECTID
        WHERE o.PORTALID=865815822 AND o.OBJECTTYPEID='0-1' AND NOT o.DELETED
        AND (NOT search('OVERFLOWPROPERTIES.string_8:john') OR l.LISTID=12)
    """

    // The virtual column rule also supports MOW; old indexed values must not leak.
    sql "DROP TABLE IF EXISTS crm_search_mow"
    sql """CREATE TABLE crm_search_mow (id BIGINT, v VARIANT,
        INDEX idx_v(v) USING INVERTED PROPERTIES("parser"="english"))
        UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true")"""
    sql """INSERT INTO crm_search_mow VALUES
        (1,parse_to_variant('{"name":"john"}')),(2,parse_to_variant('{"name":"other"}')),(3,NULL)"""
    sql """INSERT INTO crm_search_mow VALUES
        (1,parse_to_variant('{"name":"other"}')),(2,parse_to_variant('{"name":"john"}'))"""
    order_qt_mow_match_join """
        SELECT o.id, CAST(o.v['name'] AS STRING) MATCH_ANY 'john', l.k1
        FROM crm_search_mow o LEFT JOIN crm_search_full_b l ON o.id=l.k1
        WHERE CAST(o.v['name'] AS STRING) MATCH_ANY 'john' OR l.k1=1
    """
    order_qt_mow_search_join """
        SELECT o.id, l.k1
        FROM crm_search_mow o LEFT JOIN crm_search_full_b l ON o.id=l.k1
        WHERE search('v.name:john') OR l.k1=1
    """
    order_qt_search_exists_or """
        SELECT o.OBJECTID FROM crm_search_objects o
        WHERE o.PORTALID=865815822 AND o.OBJECTTYPEID='0-1' AND NOT o.DELETED
        AND (search('OVERFLOWPROPERTIES.string_8:john') OR EXISTS (
            SELECT 1 FROM crm_search_lists l WHERE l.OBJECTID=o.OBJECTID AND l.LISTID=12))
    """
    order_qt_separate_search_two_tables """
        SELECT o.OBJECTID, m.id FROM crm_search_objects o
        JOIN crm_search_mow m ON o.OBJECTID=m.id
        WHERE search('OVERFLOWPROPERTIES.string_8:john') OR search('v.name:john')
    """

    // A SEARCH field resolves like a SQL column reference: a table alias picks one side of a self join,
    // and an output alias still searches the physical column and its index.
    order_qt_search_self_join_left """
        SELECT a.k1, b.k1 FROM crm_search_full_a a JOIN crm_search_full_a b ON a.k1=b.k1
        WHERE search('a.content:hello') OR b.k1=3
    """
    order_qt_search_self_join_right """
        SELECT a.k1, b.k1 FROM crm_search_full_a a JOIN crm_search_full_a b ON a.k1=b.k1
        WHERE search('b.content:world') OR a.k1=2
    """
    test {
        sql """SELECT a.k1 FROM crm_search_full_a a JOIN crm_search_full_a b ON a.k1=b.k1
            WHERE search('content:hello')"""
        exception "Ambiguous field 'content'"
    }
    // b has k1=1 twice: the join's duplicate rows must survive the virtual column.
    order_qt_search_renamed_column """
        SELECT t.k1, b.k1 FROM (SELECT k1, content AS body FROM crm_search_full_a) t
        LEFT JOIN crm_search_full_b b ON t.k1=b.k1
        WHERE search('body:hello') OR b.k1=4
    """
    order_qt_search_renamed_variant """
        SELECT o.id, l.k1 FROM (SELECT id, v AS props FROM crm_search_mow) o
        LEFT JOIN crm_search_full_b l ON o.id=l.k1
        WHERE search('props.name:john') OR l.k1=1
    """
    // A MATCH wrapped in another expression is materialized the same way above a scan and above a join;
    // on the null-generating side it stays NULL for rows without a join partner.
    order_qt_match_case_scan """
        SELECT k1, CASE WHEN content MATCH_ANY 'hello' THEN 'hit' ELSE 'miss' END
        FROM crm_search_full_a
    """
    order_qt_match_case_join """
        SELECT a.k1, b.k1, CASE WHEN a.content MATCH_ANY 'hello' THEN 'hit' ELSE 'miss' END
        FROM crm_search_full_a a LEFT JOIN crm_search_full_b b ON a.k1=b.k1
    """
    order_qt_match_case_null_side """
        SELECT b.k1, CASE WHEN a.content MATCH_ANY 'hello' THEN 'hit'
            WHEN (a.content MATCH_ANY 'hello') IS NULL THEN 'null' ELSE 'miss' END
        FROM crm_search_full_b b LEFT JOIN crm_search_full_a a ON b.k1=a.k1
    """
    // a.content = n.name lets predicates on a.content be inferred for n.name, but a SEARCH is bound to the
    // inverted index of a.content: copied to n.name, which has no index, it would drop every joined row.
    sql "DROP TABLE IF EXISTS crm_search_names"
    sql """CREATE TABLE crm_search_names (id INT, name TEXT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO crm_search_names VALUES (10,'hello world'),(20,'hello'),(30,'world')"
    order_qt_search_not_inferred_for_equal_column """
        SELECT a.k1, n.id FROM crm_search_full_a a JOIN crm_search_names n ON a.content=n.name
        WHERE search('a.content:hello') OR a.content='zzz'
    """
    // The same holds for the positional inference of INTERSECT: the SEARCH stays in its own branch.
    order_qt_search_not_inferred_through_intersect """
        (SELECT content FROM crm_search_full_a WHERE search('content:hello'))
        INTERSECT (SELECT name FROM crm_search_names)
    """
    // A BKD (numeric) index answers only TERM/EXACT clauses, so BE returns UNKNOWN for every row of any other
    // clause. A residual SEARCH materialized as a scan virtual column must carry that UNKNOWN exactly as the
    // filter on the scan does, whatever the field's nullability: when it does not, UNKNOWN becomes FALSE and
    // NOT search(...) selects every row. Each pair below must keep matching, whichever clauses the index grows.
    sql "DROP TABLE IF EXISTS crm_search_ages"
    sql """CREATE TABLE crm_search_ages (id INT NOT NULL, age INT NOT NULL,
        INDEX idx_age(age) USING INVERTED)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO crm_search_ages VALUES (1,20),(2,25),(3,40),(4,50)"
    sql "DROP TABLE IF EXISTS crm_search_age_join"
    sql """CREATE TABLE crm_search_age_join (k1 INT)
        DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES("replication_num"="1")"""
    sql "INSERT INTO crm_search_age_join VALUES (1),(2),(3),(4)"
    order_qt_search_bkd_term_scan """
        SELECT id FROM crm_search_ages WHERE NOT search('age:20')
    """
    order_qt_search_bkd_term_join """
        SELECT a.id FROM crm_search_ages a LEFT JOIN crm_search_age_join j ON a.id = j.k1
        WHERE (NOT search('age:20')) OR j.k1 = 100
    """
    // A range clause the BKD index cannot answer: UNKNOWN, so neither shape selects a row.
    order_qt_search_bkd_range_scan """
        SELECT id FROM crm_search_ages WHERE NOT search('age:[18 TO 30]')
    """
    order_qt_search_bkd_range_join """
        SELECT a.id FROM crm_search_ages a LEFT JOIN crm_search_age_join j ON a.id = j.k1
        WHERE (NOT search('age:[18 TO 30]')) OR j.k1 = 100
    """

    // A SEARCH that cannot reach one scan is rejected instead of being evaluated without an index.
    test {
        sql """SELECT k1 FROM (SELECT k1, content FROM crm_search_full_a ORDER BY k1 LIMIT 3) t
            WHERE search('content:hello')"""
        exception "SEARCH must be evaluated by an OLAP scan"
    }
    test {
        sql """SELECT a.k1 FROM crm_search_full_a a JOIN crm_search_mow m ON a.k1=m.id
            WHERE search('content:hello OR v.name:john')"""
        exception "SEARCH must be evaluated by an OLAP scan"
    }

    // Keep SEARCH on the null-generating side gated until its full DSL NULL contract is established.
    test {
        sql """SELECT b.k1 FROM crm_search_full_b b LEFT JOIN crm_search_mow m ON b.k1=m.id
            WHERE search('NOT v.name:john') OR b.k1=8"""
        exception "SEARCH must be evaluated by an OLAP scan"
    }
    // Rewrites that NULL-pad that side (ON false, outer join to anti join) must not bypass the gate.
    test {
        sql """SELECT b.k1 FROM crm_search_full_b b LEFT JOIN crm_search_full_a a ON false
            WHERE NOT search('content:hello') OR b.k1=8"""
        exception "null-generating side of an outer join"
    }
    test {
        sql """SELECT b.k1 FROM crm_search_full_b b LEFT JOIN crm_search_full_a a ON b.k1=a.k1
            WHERE a.k1 IS NULL AND (NOT search('content:hello') OR b.k1=8)"""
        exception "null-generating side of an outer join"
    }

    // nvl(NULL, 'hello') matches: a MATCH with such an operand must stay above the outer join,
    // so rows without a join partner (b.k1 = 8 and NULL) are kept and project TRUE.
    sql "set enable_match_without_inverted_index = true"
    order_qt_nullside_nonstrict_match_where """
        SELECT b.k1 FROM crm_search_full_b b LEFT JOIN crm_search_full_a a ON b.k1=a.k1
        WHERE nvl(a.content, 'hello') MATCH_ANY 'hello' OR b.k1=100
    """
    order_qt_nullside_nonstrict_match_select """
        SELECT b.k1, nvl(a.content, 'hello') MATCH_ANY 'hello'
        FROM crm_search_full_b b LEFT JOIN crm_search_full_a a ON b.k1=a.k1
    """
    sql "set enable_match_without_inverted_index = false"

}
