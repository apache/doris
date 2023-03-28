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

suite("test_join_result_count", "query,p0") {
    sql """
drop table if exists t1;
"""

    sql """
CREATE TABLE `t1` (
`c0` datev2 NULL ,
`key` varchar(40) NULL ,
`c2` varchar(40) NULL ,
`c3` char(1) NULL ,
`c4` varchar(4) NULL ,
`c5` varchar(2) NULL ,
`c6` varchar(2) NULL ,
`c7` varchar(8) NULL ,
`c8` varchar(8) NULL ,
`c9` char(1) NULL ,
`c10` char(1) NULL ,
`c11` char(1) NULL ,
`c12` char(1) NULL ,
`c13` char(1) NULL ,
`c14` char(1) NULL ,
`c15` char(1) NULL ,
`c16` char(1) NULL ,
`c17` char(1) NULL ,
`c18` char(1) NULL ,
`c19` char(1) NULL ,
`c20` char(1) NULL ,
`c21` char(1) NULL ,
`c22` char(1) NULL ,
`c23` char(1) NULL ,
`c24` char(1) NULL ,
`c25` varchar(8) NULL ,
`c26` varchar(8) NULL ,
`c27` varchar(8) NULL ,
`c28` varchar(8) NULL ,
`c29` varchar(8) NULL ,
`c30` varchar(8) NULL ,
`c31` varchar(8) NULL ,
`c32` varchar(8) NULL ,
`c33` varchar(8) NULL ,
`c34` varchar(100) NULL ,
`c35` datev2 NULL ,
`c36` datev2 NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `key`)
DISTRIBUTED BY HASH(`c0`, `key`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false"
)
"""

    sql """
drop table if exists t2
"""

    sql """
CREATE TABLE `t2` (
`d0` varchar(40) NULL,
`d1` varchar(40) NULL,
`d2` varchar(40) NULL,
`d3` varchar(40) NULL,
`d4` varchar(40) NULL,
`d5` varchar(40) NULL,
`d6` datev2 NULL,
`d7` datev2 NULL,
`d8` varchar(300) NULL,
`d9` varchar(100) NULL,
`d10` varchar(8) NULL,
`d11` varchar(100) NULL,
`d12` varchar(8) NULL,
`d13` varchar(100) NULL,
`d14` varchar(8) NULL,
`d15` varchar(100) NULL,
`d16` varchar(8) NULL,
`d17` varchar(100) NULL,
`d18` varchar(8) NULL,
`d19` varchar(100) NULL,
`d20` decimalv3(10, 4) NULL,
`d21` varchar(3) NULL,
`d22` varchar(8) NULL,
`d23` decimalv3(19, 4) NULL,
`d24` decimalv3(31, 6) NULL,
`d25` decimalv3(24, 4) NULL,
`d26` decimalv3(24, 4) NULL,
`d27` varchar(200) NULL,
`d28` varchar(200) NULL,
`d29` decimalv3(31, 6) NULL,
`d30` decimalv3(31, 6) NULL,
`d31` varchar(2) NULL,
`d32` varchar(50) NULL,
`d33` varchar(2) NULL,
`d34` varchar(2) NULL,
`d35` varchar(50) NULL,
`d36` decimalv3(10, 0) NULL,
`d37` varchar(120) NULL,
`d38` varchar(2) NULL,
`d39` varchar(2) NULL,
`d40` varchar(2) NULL,
`d41` varchar(2) NULL,
`d42` varchar(2) NULL,
`d43` varchar(2) NULL,
`d44` varchar(2) NULL,
`d45` varchar(2) NULL,
`d46` varchar(2) NULL,
`d47` varchar(2) NULL,
`d48` varchar(2) NULL,
`d49` varchar(2) NULL,
`d50` varchar(2) NULL,
`d51` varchar(40) NULL,
`d52` varchar(100) NULL,
`d53` varchar(40) NULL,
`d54` varchar(200) NULL,
`d55` varchar(200) NULL,
`d56` varchar(200) NULL,
`d57` datev2 NULL,
`d58` datev2 NULL,
`d59` datev2 NULL,
`d60` datev2 NULL,
`d61` datev2 NULL,
`d62` datev2 NULL,
`d63` datev2 NULL,
`d64` varchar(200) NULL,
`d65` varchar(200) NULL,
`d66` varchar(20) NULL,
`d67` varchar(20) NULL,
`d68` datev2 NULL,
`d69` datev2 NULL,
`d70` decimalv3(20, 6) NULL,
`d71` decimalv3(20, 6) NULL,
`d72` varchar(50) NULL,
`c33` varchar(8) NULL,
`c34` varchar(100) NULL,
`c35` datev2 NULL,
`c36` datev2 NULL,
`d77` varchar(50) NULL
) ENGINE=OLAP
DUPLICATE KEY(`d0`)
DISTRIBUTED BY HASH(`d0`) BUCKETS 1
PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"light_schema_change" = "true",
"disable_auto_compaction" = "false"
);
"""

    sql """
INSERT INTO t1 VALUES 
('2010-01-01','001','5J','N','c2','02','29','cI','k2','2','f','R','e','H','6','0','f','4','M','v','v','V','r','V','P','wC','YU','8I','Bx','Sn','dM','5D','O7','QIA','QO','2010-01-01','2010-01-01'),
('2010-01-01','001','sX','N','H5','02','29','B8','WD','A','n','s','9','G','K','3','x','Y','5','U','r','3','z','z','f','Cg','K7','Yh','Zq','4x','6G','rH','J3','WND','zs','2010-01-01','2010-01-01'),
('2010-01-01','001','Eo','x','DX','02','29','65','0h','H','m','B','0','f','n','9','n','L','M','N','E','l','T','P','s','8H','E7','OV','zz','Di','Sy','8T','qG','WND','7a','2010-01-01','2010-01-01'),
('2010-01-01','001','Cd','f','Ub','02','29','R1','O2','B','O','V','B','N','i','U','R','e','P','q','x','U','i','r','3','uC','Qz','Yy','jB','je','DX','N3','MQ','WND','Ce','2010-01-01','2010-01-01'),
('2010-01-01','001','uS','h','Vs','02','29','Zv','VD','9','5','D','L','u','S','A','F','f','7','A','r','f','m','5','a','V0','Xd','x4','pv','Es','H2','d3','MQ','WND','y3','2010-01-01','2010-01-01'),
('2010-01-01','001','5Q','Z','6I','02','29','zs','Fn','I','j','0','7','S','v','V','Q','L','1','W','W','y','1','t','u','2i','Md','of','SF','j2','Jm','9L','9L','NIA','Qh','2010-01-01','2010-01-01'),
('2010-01-01','001','FA','2','uX','02','29','sc','i1','B','4','1','3','k','f','E','V','S','j','v','C','g','f','o','v','H4','8c','Rt','lZ','nN','8y','1n','1U','GIL','kB','2010-01-01','2010-01-01'),
('2010-01-01','001','H1','o','ja','02','29','Ln','Tg','T','I','O','s','9','N','g','r','C','J','x','T','J','o','D','0','Z7','Z2','p7','d2','Ky','I7','ml','X0','NIA','Fx','2010-01-01','2010-01-01'),
('2010-01-01','001','Zv','f','Jk','02','29','zc','lN','6','R','h','v','F','h','U','r','2','i','W','v','o','q','h','m','cJ','Ho','tK','ob','g3','TQ','lM','2h','GIL','hG','2010-01-01','2010-01-01'),
('2010-01-01','001','VS','v','ix','02','29','K2','aq','H','4','d','e','6','y','H','2','J','Z','n','5','w','G','u','G','az','KT','xA','iZ','ug','La','jE','qb','QIA','cs','2010-01-01','2010-01-01')
"""

    sql """
INSERT INTO t2 VALUES ('BND004101900107','AF','5Z','WO','Na','53','2010-01-01','2010-01-01','Uz','3i','KD','MD','YX','1X','J0','Q1','Cv','XS','gJ','AJ',55.8319,'UL','65',70.9600,0.134744,23.6171,21.6050,'uA','me',69.812478,32.178012,'ah','9A','ff','BG','qG',50,'8g','VN','Z1','dl','vW','zi','K9','Y8','uX','W2','c7','Ec','Ol','R8','CO','hd','CO','Ki','tx','wx','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','Uw','Fu','bD','ox','2010-01-01','2010-01-01',48.611671,43.375518,'1','QIA','J2','2010-01-01','2010-01-01','QJ'),
('BND004101900107','qz','9C','Ld','Qz','Eb','2010-01-01','2010-01-01','3U','eo','BV','k6','nA','at','Fk','zN','oh','6S','pD','JH',15.8854,'gq','Xl',28.6945,33.605846,10.1608,14.7693,'I1','5p',74.201643,24.743211,'ON','GY','N6','KZ','LC',38,'U2','gL','Sv','WM','tC','xH','7Q','RH','my','dU','2L','pr','0H','JX','CO','Zd','CO','BT','Au','WL','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','1X','RK','Xj','ZA','2010-01-01','2010-01-01',75.329020,89.149149,'1','WND','9Y','2010-01-01','2010-01-01','l9'),
('BND004101900107','kg','Mx','C6','mo','sn','2010-01-01','2010-01-01','8T','Vz','HY','j2','ko','Cr','iD','VE','2y','5A','p7','fE',47.7432,'8r','jB',96.3349,45.576238,79.1633,95.3908,'SZ','H3',81.117160,41.821358,'zP','gH','re','ot','J2',54,'rW','mI','dA','vL','FE','OB','JF','w6','Gc','W6','HO','97','NB','A1','CO','tY','CO','ny','lj','a8','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2K','Mj','QQ','Ub','2010-01-01','2010-01-01',32.776935,41.901579,'1','WND','mr','2010-01-01','2010-01-01','fq'),
('BND004101900107','dX','3j','gr','d4','x9','2010-01-01','2010-01-01','E9','dV','qB','Rh','tk','9S','kc','pS','Vn','jE','l8','Ip',57.6834,'64','i7',1.4679,69.714925,78.5287,77.7748,'OQ','q3',75.203826,62.194713,'OL','J1','8A','cA','zG',52,'BE','xk','Ec','eL','A5','Su','pH','jE','rX','sR','nG','yo','aK','L3','CO','mu','CO','dC','kz','Xb','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','90','ez','7f','34','2010-01-01','2010-01-01',42.702910,90.962041,'1','WND','JC','2010-01-01','2010-01-01','xb'),
('BND004101900107','zd','LI','zT','04','Bm','2010-01-01','2010-01-01','iW','yx','kw','lB','L2','T8','Y5','lX','u4','sY','KI','Ak',17.1610,'h8','S0',64.6265,70.828091,12.1270,36.9771,'dF','Sc',78.890043,47.431674,'l1','eM','Ti','uo','Du',16,'DL','EA','XG','xQ','4e','hm','53','JI','sk','cU','N4','Pf','7u','IM','CO','dJ','CO','nY','cf','Cy','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','KS','ZU','eA','Mh','2010-01-01','2010-01-01',34.823438,3.743677,'1','QIA','gI','2010-01-01','2010-01-01','a1'),
('BND004101900107','GT','xE','1m','Rh','Py','2010-01-01','2010-01-01','aM','d6','e7','Ws','qD','31','cU','Ch','E9','X0','R5','tP',2.7447,'WV','3u',66.6396,96.289522,11.7877,19.8887,'PU','xQ',56.512539,0.304057,'F5','FO','ZS','JA','V8',86,'1A','71','Me','D5','w0','9z','tq','B8','lX','Tq','O6','i9','pT','PY','CO','cb','CO','7t','rJ','xB','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','gH','my','cU','Fu','2010-01-01','2010-01-01',4.538020,47.614570,'1','NIA','x1','2010-01-01','2010-01-01','mw'),
('BND004101900107','0u','36','g7','DT','Qu','2010-01-01','2010-01-01','2r','Eg','kD','jn','8U','vi','6x','1U','uU','fE','ow','BF',56.9681,'m1','EF',28.2181,43.594374,6.3818,78.2836,'gE','X3',48.518390,95.914323,'It','5g','pY','rX','0L',75,'sK','lC','sH','XY','UD','hE','fE','w2','AY','3m','rF','cC','cK','Vk','CO','lC','CO','hs','hb','Yt','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','TF','nt','N8','4J','2010-01-01','2010-01-01',29.665762,74.514460,'1','WND','sl','2010-01-01','2010-01-01','pr'),
('BND004101900107','up','wE','9w','0p','9O','2010-01-01','2010-01-01','qN','2C','tA','6s','mr','VI','sf','RT','Gd','jY','vd','Jd',40.7267,'pS','dH',3.6965,71.195587,9.6647,94.4072,'dK','tt',16.196071,66.302767,'oV','dE','60','gj','75',99,'3x','MG','VR','mc','FE','ZS','vm','OO','pR','93','CF','5r','aQ','sa','CO','fP','CO','0u','Za','Sj','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','tI','gp','AQ','BA','2010-01-01','2010-01-01',79.135621,4.571175,'1','QIA','Po','2010-01-01','2010-01-01','p2'),
('BND004101900107','P0','DQ','Dj','gY','7j','2010-01-01','2010-01-01','Cb','zv','WE','xR','SR','p1','7G','rs','Hv','Ng','Z5','hr',92.3492,'ie','Vd',62.9489,54.393932,55.1925,92.5064,'d7','n6',94.324108,62.739001,'Ej','UN','LQ','Un','pX',60,'0p','Io','fP','i9','ct','2U','ko','vs','Ui','xa','f4','7U','Px','ha','CO','q6','CO','x8','dP','rY','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','5W','8S','3Z','0R','2010-01-01','2010-01-01',2.485433,69.855650,'1','GIL','R4','2010-01-01','2010-01-01','bm'),
('BND004101900107','kA','pC','I8','kB','0S','2010-01-01','2010-01-01','Ce','lM','Hb','6e','IW','ag','vD','Sl','MP','Li','hG','v8',8.6618,'Ek','jF',1.6873,83.454653,85.9190,62.7885,'NC','xo',68.356872,10.699634,'J5','6I','ZG','rd','Vy',49,'pK','Ka','Kp','xO','GX','2T','n8','4a','el','BB','Ry','Xh','yw','5w','CO','oa','CO','4N','Pu','x9','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2010-01-01','2I','PS','mK','hd','2010-01-01','2010-01-01',47.467408,8.705185,'1','WND','qg','2010-01-01','2010-01-01','hw')  
"""

    test {
        sql """
SELECT * FROM t2 a LEFT JOIN t1 b ON b.`key` = '001'
"""
        rowNum 100
    }
}
