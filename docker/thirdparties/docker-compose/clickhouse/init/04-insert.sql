-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

INSERT INTO doris_test.type VALUES
(true, '2022-01-01','2022-01-01','2022-01-01 00:00:00','2022-01-01 00:00:00.000000000',1.1,1.1,1,1,1,1,1,1,1,1,1,1,1,1,1.1,1.1,1.1,1.1,1,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','F');
INSERT INTO doris_test.type VALUES
(false, '2022-01-02','2022-01-02','2022-01-02 00:00:00','2022-01-02 00:00:00.000000000',2.2,2.2,2,2,2,2,2,2,2,2,2,2,2,2,2.2,2.2,2.2,2.2,2,'116.253.40.133','2a02:aa08:e000:3100::2','61f0c404-5cb3-11e7-907b-a6006ad3dba0','String','T');

INSERT INTO doris_test.student values (1, 'doris', 18), (2, 'alice', 19), (3, 'bob', 20);

INSERT INTO doris_test.arr values
('1',[true],['2022-01-01'],['2022-01-01'],[1.1],[1.1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[1],[2.2],[1],['116.253.40.133'],['2a02:aa08:e000:3100::2'],['61f0c404-5cb3-11e7-907b-a6006ad3dba0'],[1],['string'],['string'],['2022-01-01 00:00:00'],['2022-01-01 00:00:00'])