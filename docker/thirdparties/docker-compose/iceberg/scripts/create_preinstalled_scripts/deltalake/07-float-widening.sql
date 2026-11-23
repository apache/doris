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

CREATE OR REPLACE TABLE delta_m1.float_widening (id INT, p FLOAT, v FLOAT)
USING delta PARTITIONED BY (p) LOCATION 's3a://delta/warehouse/delta_m1/float_widening'
TBLPROPERTIES ('delta.enableTypeWidening'='true', 'delta.enableDeletionVectors'='false');

-- The first file stores FLOAT 0.1 while its JSON min and max are both 0.1.
INSERT OVERWRITE delta_m1.float_widening VALUES
    (1, CAST('0.1' AS FLOAT), CAST('0.1' AS FLOAT)),
    (2, CAST('1.25' AS FLOAT), CAST('1.25' AS FLOAT));
ALTER TABLE delta_m1.float_widening ALTER COLUMN p TYPE DOUBLE;
ALTER TABLE delta_m1.float_widening ALTER COLUMN v TYPE DOUBLE;
INSERT INTO delta_m1.float_widening VALUES (3, CAST('0.1' AS DOUBLE), CAST('0.1' AS DOUBLE));
