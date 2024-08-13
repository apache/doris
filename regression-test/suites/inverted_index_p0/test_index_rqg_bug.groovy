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
suite("test_index_rqg_bug", "test_index_rqg_bug"){
    def table1 = "test_index_rqg_bug"

    sql "drop table if exists ${table1}"

    sql """
    CREATE TABLE ${table1} (
        col_date_undef_signed_not_null DATE NOT NULL,
        col_bigint_undef_signed_not_null_index_inverted BIGINT NOT NULL,
        col_bigint_undef_signed_not_null BIGINT NOT NULL,
        col_int_undef_signed INT NULL,
        col_int_undef_signed_index_inverted INT NULL,
        col_int_undef_signed_not_null INT NOT NULL,
        col_int_undef_signed_not_null_index_inverted INT NOT NULL,
        col_bigint_undef_signed BIGINT NULL,
        col_bigint_undef_signed_index_inverted BIGINT NULL,
        col_date_undef_signed DATE NULL,
        col_date_undef_signed_index_inverted DATE NULL,
        col_date_undef_signed_not_null_index_inverted DATE NOT NULL,
        col_varchar_10__undef_signed VARCHAR(10) NULL,
        col_varchar_10__undef_signed_index_inverted VARCHAR(10) NULL,
        col_varchar_10__undef_signed_not_null VARCHAR(10) NOT NULL,
        col_varchar_10__undef_signed_not_null_index_inverted VARCHAR(10) NOT NULL,
        col_varchar_1024__undef_signed VARCHAR(1024) NULL,
        col_varchar_1024__undef_signed_index_inverted VARCHAR(1024) NULL,
        col_varchar_1024__undef_signed_not_null VARCHAR(1024) NOT NULL,
        col_varchar_1024__undef_signed_not_null_index_inverted VARCHAR(1024) NOT NULL,
        pk INT,
        INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_index_inverted_idx (`col_date_undef_signed_index_inverted`) USING INVERTED,
        INDEX col_date_undef_signed_not_null_index_inverted_idx (`col_date_undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_10__undef_signed_index_inverted_idx (`col_varchar_10__undef_signed_index_inverted`) USING INVERTED,
        INDEX col_varchar_10__undef_signed_not_null_index_inverted_idx (`col_varchar_10__undef_signed_not_null_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_index_inverted_idx (`col_varchar_1024__undef_signed_index_inverted`) USING INVERTED,
        INDEX col_varchar_1024__undef_signed_not_null_index_inverted_idx (`col_varchar_1024__undef_signed_not_null_index_inverted`) USING INVERTED
    ) ENGINE=OLAP
    UNIQUE KEY(col_date_undef_signed_not_null, col_bigint_undef_signed_not_null_index_inverted, col_bigint_undef_signed_not_null)
    PARTITION BY RANGE(col_date_undef_signed_not_null) (
        PARTITION p1 VALUES LESS THAN ('2024-01-01'),
        PARTITION p2 VALUES LESS THAN ('2024-12-01'),
        PARTITION p3 VALUES LESS THAN ('2027-01-01'),
        PARTITION p4 VALUES LESS THAN ('2032-01-01')
    )
    DISTRIBUTED BY HASH(col_bigint_undef_signed_not_null)
    PROPERTIES (
        "enable_unique_key_merge_on_write" = "true",
        "replication_num" = "1"
    );
    """

    sql """
    INSERT INTO ${table1} (
        pk,
        col_int_undef_signed,
        col_int_undef_signed_index_inverted,
        col_int_undef_signed_not_null,
        col_int_undef_signed_not_null_index_inverted,
        col_bigint_undef_signed,
        col_bigint_undef_signed_index_inverted,
        col_bigint_undef_signed_not_null,
        col_bigint_undef_signed_not_null_index_inverted,
        col_date_undef_signed,
        col_date_undef_signed_index_inverted,
        col_date_undef_signed_not_null,
        col_date_undef_signed_not_null_index_inverted,
        col_varchar_10__undef_signed,
        col_varchar_10__undef_signed_index_inverted,
        col_varchar_10__undef_signed_not_null,
        col_varchar_10__undef_signed_not_null_index_inverted,
        col_varchar_1024__undef_signed,
        col_varchar_1024__undef_signed_index_inverted,
        col_varchar_1024__undef_signed_not_null,
        col_varchar_1024__undef_signed_not_null_index_inverted
    ) VALUES
        (0, 4, -10, -10, 1, 4110874392796006003, NULL, -1498307980504297658, -6624530927695828604, '2023-12-16', '2024-02-18', '2025-02-17', '2026-02-18', 's', NULL, 'm', 'k', NULL, 'x', 'k', 'h'),
        (1, -4, 1, 1, 3, 6623681431777179589, 7716120692706688082, 615786308551882497, 4891738753710194253, '2023-12-14', '2023-12-17', '2024-01-19', '2023-12-16', NULL, 'w', 'i', 'x', 'w', NULL, 'u', 'f'),
        (2, NULL, -4, 4, 1, -6317401069474992907, NULL, -7617717703626563584, 2235475868764729941, '2026-02-18', '2023-12-17', '2025-02-18', '2023-12-11', 'c', 'b', 'c', 'e', NULL, 'r', 'd', 'b'),
        (3, 5, 7, 6, -10, -8764524698563027859, NULL, -6887301029682450547, 5745000062733084993, NULL, '2023-12-13', '2026-02-18', '2024-01-08', 'r', NULL, 'u', 'b', NULL, 'x', 'f', 't'),
        (4, 5, 6, 5, 5, 1207999763325556334, 400406972022333702, 4909325697798323112, 8228207078332149326, '2023-12-09', '2024-02-18', '2024-02-18', '2023-12-12', 'b', 'y', 'r', 'k', 'p', NULL, 'w', 'u'),
        (5, NULL, NULL, 9, 7, -399926527043614504, -6727343589196278399, 2123878411701229252, 3537945973941774192, '2023-12-20', '2026-02-18', '2023-12-13', '2025-02-18', 'f', NULL, 'b', 'u', 'v', 'q', 'x', 'z'),
        (6, 5, -4, -4, -4, 7176944242542256823, NULL, -6890476710374490789, 6102795527953951961, '2024-02-18', '2023-12-10', '2025-06-18', '2024-02-18', 'w', 'w', 'n', 'f', 'j', 'c', 'z', 'j'),
        (7, NULL, 6, 4, 9, 1574705768051310419, -8158588360690951984, -8654147861551576678, 9013730942726046271, '2024-01-08', '2023-12-20', '2025-02-17', '2024-02-18', 'w', 'o', 'r', 'g', NULL, 'd', 'f', 'f'),
        (8, 2, 7, 1, 7, 2562431938655417541, -7688325706557454517, 1079974766952131196, 1734253071879102419, '2027-01-16', '2024-02-18', '2023-12-09', '2023-12-18', 'p', NULL, 'd', 'e', NULL, 'd', 'l', 'g'),
        (9, -10, -10, 4, 2, 6713759377679609112, -5731114671498415638, 6931063896689364820, 234584371629600890, '2023-12-17', '2024-02-18', '2025-06-18', '2024-02-18', 'r', 't', 'z', 'i', NULL, 'o', 't', 'r'),
        (10, 7, -4, 7, 0, -8748876285137363613, 1038125288026833770, -6467315269727361595, -2832936617000850958, '2023-12-14', '2023-12-19', '2023-12-12', '2024-02-18', NULL, NULL, 'h', 's', NULL, 'i', 'w', 'a'),
        (11, NULL, 0, 2, -10, NULL, -4301420623572732841, -4077495118410655984, -7539371126512153610, '2023-12-11', '2025-06-18', '2023-12-12', '2027-01-16', 'h', 'g', 'i', 't', 'c', 'o', 't', 'm'),
        (12, 2, -10, 8, 9, -1593689086447063667, NULL, -7566059580459201103, 3617662068101719443, '2024-01-09', '2024-01-19', '2024-01-09', '2024-01-08', 'e', 'z', 'q', 'q', 'd', 'a', 'a', 'l'),
        (13, NULL, 4, -4, 1, -5708165596259700149, -5685726923171048111, 9066963508989307621, 2458783504808594136, '2023-12-10', '2023-12-13', '2023-12-18', '2026-01-18', 'p', 't', 'j', 'q', NULL, 'e', 'u', 'm'),
        (14, 7, 7, -10, 9, -7962935318866216216, -4953801555403517032, 8523153274499575118, 1613114031483806037, '2025-06-18', '2024-01-19', '2023-12-12', '2023-12-10', 'v', 's', 'o', 'j', 'b', 'f', 'q', 'w'),
        (15, 6, 0, 8, 0, 6328665641116017087, NULL, 5391031021957467297, 5462908293080729447, '2026-02-18', '2023-12-09', '2023-12-18', '2024-02-18', 'm', NULL, 'n', 'o', NULL, 's', 's', 's'),
        (16, 6, 2, -4, 3, -8834453054854906553, 6419622466258448361, -1077284003289783079, 1766860833711435197, '2023-12-13', '2025-02-17', '2023-12-13', '2024-01-19', NULL, NULL, 'b', 'k', 'o', 'z', 'z', 'w'),
        (17, NULL, 8, -4, 6, 3565612934284736788, NULL, 5161435793488169413, 8261046922674753150, '2024-01-09', '2024-01-08', '2023-12-17', '2025-06-18', 'b', NULL, 'd', 'r', NULL, NULL, 'i', 'b'),
        (18, 0, 8, 6, 6, 4375516907761154738, -4335939766709274978, -5059966477520599003, 3808923561777121597, '2023-12-13', '2023-12-18', '2023-12-11', '2024-01-19', 'l', 'p', 'y', 's', 'z', NULL, 'e', 'z'),
        (19, 7, NULL, 6, -4, 4255812431146886434, -3567263479012631213, -3181821391660277576, -3071346623506336536, '2023-12-11', '2023-12-17', '2026-01-18', '2023-12-14', 'n', 'c', 'g', 'e', 'u', 't', 'h', 'g'),
        (20, -4, -10, 6, -4, 2652092880342363868, -8920289924092502353, -5849438024240178915, 8184871629616434884, '2025-06-18', '2023-12-17', '2026-02-18', '2023-12-17', 'a', NULL, 'a', 'k', 'm', 'y', 'j', 'v'),
        (21, NULL, NULL, 1, 0, 6948549308905807097, NULL, -4500055558544887047, -3290043767973638972, '2023-12-11', NULL, '2024-02-18', '2024-01-19', 'j', 'f', 'i', 'a', 'e', 'q', 'c', 'h'),
        (22, -4, -4, 9, 2, NULL, -158815691327716354, -6461411464053190264, -2252322951397765832, '2023-12-18', '2026-01-18', '2023-12-13', '2023-12-17', 'n', 'e', 'w', 'v', 'o', NULL, 'd', 'd'),
        (23, -4, 4, 3, 9, -8610148868034222680, -1819499259609527905, -1843720688790269680, 8916201680455198129, NULL, '2025-06-18', '2025-06-18', '2026-02-18', 'b', 't', 'k', 'g', 'h', 'w', 'w', 'l'),
        (24, 6, 3, 8, -10, 5265421523550985132, NULL, 1985005821921806432, -622793106644431500, '2025-02-18', '2023-12-13', '2023-12-19', '2023-12-19', 'w', 'b', 'p', 't', 'f', NULL, 'k', 'w'),
        (25, NULL, 9, -10, -10, 7589949497405213275, -4340958604720476959, 4049046233331066630, 2898537274810600857, '2024-01-31', '2025-06-18', '2023-12-15', '2023-12-09', 'e', 'u', 'e', 'j', 't', 'q', 'p', 's'),
        (26, 8, 5, 4, -10, NULL, -5313196753564902712, 3653082909484769862, 4903302434698132353, '2023-12-19', '2023-12-13', '2025-02-18', '2024-02-18', 'v', 'g', 'h', 'z', 'y', 'b', 'i', 's'),
        (27, NULL, 0, -4, -4, -2381979625674393471, 5800633531765481430, 5093367841651274621, -8275343541296293718, '2024-02-18', '2023-12-13', '2024-01-19', '2024-01-09', 'x', 'a', 'r', 'v', 'f', 'q', 'y', 'p'),
        (28, 9, -4, -10, 2, 897003576941309512, 7855179910637993791, 1332910455783140619, 5623368437141522090, '2024-01-31', '2024-02-18', '2023-12-11', '2024-01-19', 'x', 'x', 'g', 'f', 'c', 'i', 'k', 'k'),
        (29, -10, 6, -4, 7, 7666235135687994466, -6020517871725562593, -5548637543778191066, -4672170603609776026, '2023-12-11', '2023-12-11', '2024-01-31', '2025-02-17', NULL, 's', 'q', 'd', 'w', NULL, 'x', 'm'),
        (30, -10, NULL, -4, 1, -1886054769267203982, NULL, -6353719426475900661, -6176524628199292596, '2026-01-18', '2024-01-08', '2023-12-14', '2024-01-19', 'j', 'm', 'l', 'e', 'q', 'q', 'r', 'q'),
        (31, -10, 6, 3, 7, NULL, NULL, -2863540841136593771, -8254138790633197636, '2023-12-19', '2024-01-08', '2025-06-18', '2025-06-18', NULL, 'b', 'h', 'q', 'o', 'p', 'w', 'd'),
        (32, NULL, 3, 4, 7, NULL, 1174004179679873270, 8070004279648365140, -829180675569535262, NULL, '2024-01-09', '2023-12-17', '2023-12-12', 'l', 'i', 'j', 'y', 'r', 'a', 'u', 'z'),
        (33, -10, NULL, 4, -4, 3584152679446786688, 3109569438292629584, -3834045297758156664, -4243139038606929981, '2027-01-09', '2023-12-17', '2024-02-18', '2026-02-18', NULL, 'r', 'a', 'v', NULL, 'o', 'j', 'k'),
        (34, 1, -10, 1, 7, NULL, 2963572831123067573, -3467524118995743827, 6313552142892095400, '2023-12-11', '2024-01-19', '2025-02-17', '2023-12-14', 'h', 'z', 'x', 'h', 'u', 'z', 'l', 'p'),
        (35, -4, -4, 7, -4, 6918317195863910307, NULL, -3914983881398507267, -1682813683244761601, '2023-12-10', '2025-06-18', '2023-12-14', '2023-12-19', 'f', 'l', 'd', 'w', 'k', 's', 'i', 'c'),
        (36, 4, NULL, -10, 7, -6877519807552972983, -3430825290513925147, -1216926548767692449, 9042621909063033065, '2023-12-16', '2023-12-15', '2023-12-12', '2025-06-18', 'c', 'm', 'l', 'h', 'y', 'b', 'l', 'v'),
        (37, -4, 6, 1, 0, 7689721283456404303, -5255418269014510711, 8535151098392979993, -441360284002757950, '2023-12-20', '2023-12-12', '2024-02-18', '2023-12-18', 'c', 'y', 'x', 'v', 'm', 'i', 'z', 'n'),
        (38, 6, 2, 6, 3, -7333120475687388168, -1069033230977569687, 7807535113257942910, 5984932105630043487, '2025-06-18', '2024-01-19', '2026-02-18', '2024-01-17', 'r', 'a', 'i', 'p', 'y', 's', 'z', 'h'),
        (39, 5, 7, 7, 7, 6421220507617800512, -3533153783116044577, -239316490331624849, 8241216535942171163, '2023-12-19', '2024-01-08', '2023-12-12', '2023-12-17', NULL, 'q', 'h', 'x', NULL, 'b', 'm', 'a'),
        (40, 8, -10, -10, -10, -4574695328152397851, -6955837295380248400, -4244395419978913928, 9047025262714422095, '2023-12-18', '2024-02-18', '2024-01-17', '2024-02-18', 'g', 'k', 'i', 'q', 'n', 'z', 't', 'x'),
        (41, 0, -10, 4, 8, NULL, NULL, 3209611308894921252, 8711702857960621012, '2023-12-20', NULL, '2025-02-17', '2023-12-20', 'e', 'l', 'e', 'f', 'b', 'g', 'r', 'q'),
        (42, 7, 5, 4, -10, 960020364402367943, -2653361156952746836, -4112703310968916635, 1180710471873346557, '2024-01-08', '2023-12-11', '2023-12-14', '2024-02-18', 'x', 'f', 't', 'u', 's', 'i', 'm', 's'),
        (43, 1, 9, -10, 5, -5818167584349938837, -8132519247916803713, 3295188548139467274, -5334220785061585644, NULL, '2023-12-16', '2023-12-19', '2023-12-17', 'g', NULL, 'j', 'b', 'w', 'a', 'h', 'a'),
        (44, -10, 1, 2, 4, NULL, 5934792409667886069, 4758869126884129938, -4944697208174184384, '2024-02-18', '2023-12-13', '2026-02-18', '2023-12-16', 'c', NULL, 'm', 'o', 's', 't', 'n', 'n'),
        (45, 2, 7, 8, 4, -2336432070277443377, 8773374274628530303, -33067191700513568, 3707683561436853007, '2023-12-11', '2024-01-09', '2026-02-18', '2026-02-18', 'c', NULL, 's', 'v', 'b', 'k', 'p', 'k'),
        (46, 3, NULL, -4, -4, -6971502840094747127, 9134675755720912429, -5073100406526280007, -302110619284724855, '2027-01-09', '2023-12-15', '2023-12-19', '2023-12-10', NULL, NULL, 'l', 't', 'b', 'e', 'c', 'f'),
        (47, 0, -4, -4, 9, -4836232184860990542, 6091190453908096642, 7748205419220794835, 6734586237025330735, '2024-01-08', '2023-12-18', '2023-12-18', '2027-01-16', NULL, 'w', 'v', 'q', 'y', 'm', 'z', 't'),
        (48, -10, 7, 3, 4, -1290467110130692882, NULL, -5421887030808227301, 2147894047624029750, '2023-12-20', '2026-02-18', '2023-12-10', '2024-02-18', 'v', 'f', 'u', 'z', 'w', 'l', 'i', 'b'),
        (49, 4, -10, -10, -4, 7177870619817484302, 2010854013707344984, 515636226818986547, -4617727694631456148, '2023-12-14', '2024-01-09', '2023-12-11', '2024-01-08', 'k', 'o', 'r', 'h', 'x', 'v', 'm', 'r');
    """

    qt_select_bug_1 """
    SELECT 
        MIN(DISTINCT table1.col_date_undef_signed_not_null) AS field1, 
        TO_DATE(
            CASE 
                WHEN table1.col_date_undef_signed_not_null != DATE_ADD(table1.col_date_undef_signed_not_null_index_inverted, INTERVAL 7 DAY) 
                THEN DATE_SUB(table1.col_date_undef_signed_not_null_index_inverted, INTERVAL 8 DAY) 
                ELSE DATE_ADD(table1.col_date_undef_signed_not_null, INTERVAL 365 DAY) 
            END
        ) AS field2
    FROM 
        ${table1} AS table1
    WHERE 
        ( 
            (table1.col_int_undef_signed_not_null_index_inverted != 6) 
            OR table1.col_date_undef_signed_not_null NOT IN ('2018-11-14') 
            OR table1.col_int_undef_signed_index_inverted <> 9
        ) 
    GROUP BY 
        field2  
    ORDER BY 
        field2 
    LIMIT 1000 
    OFFSET 5;
    """

}
