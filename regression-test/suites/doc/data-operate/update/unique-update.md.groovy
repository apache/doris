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

import org.junit.jupiter.api.Assertions;

suite("docs/data-operate/update/unique-update.md") {
    try {
        multi_sql """
            DROP TABLE IF EXISTS transaction_details;
            CREATE TABLE transaction_details (
              transaction_id BIGINT NOT NULL,        -- Unique transaction ID
              user_id BIGINT NOT NULL,               -- User ID
              transaction_date DATE NOT NULL,        -- Transaction date
              transaction_time DATETIME NOT NULL,    -- Transaction time
              transaction_amount DECIMAL(18, 2),     -- Transaction amount
              transaction_device STRING,             -- Transaction device
              transaction_region STRING,             -- Transaction region
              average_daily_amount DECIMAL(18, 2),   -- Average daily transaction amount over the last 3 months
              recent_transaction_count INT,          -- Number of transactions in the last 7 days
              has_dispute_history BOOLEAN,           -- Whether there is a dispute history
              risk_level STRING                      -- Risk level
            )
            UNIQUE KEY(transaction_id)
            DISTRIBUTED BY HASH(transaction_id) BUCKETS 10
            PROPERTIES (
              "replication_num" = "3",               -- Number of replicas, default is 3
              "enable_unique_key_merge_on_write" = "true"  -- Enable MOW mode, support merge update
            );

            INSERT INTO transaction_details VALUES
            (1001, 5001, '2024-11-24', '2024-11-24 14:30:00', 100.00, 'iPhone 12', 'New York', 100.00, 10, false, NULL),
            (1002, 5002, '2024-11-24', '2024-11-24 03:30:00', 120.00, 'iPhone 12', 'New York', 100.00, 15, false, NULL),
            (1003, 5003, '2024-11-24', '2024-11-24 10:00:00', 150.00, 'Samsung S21', 'Los Angeles', 100.00, 30, false, NULL),
            (1004, 5004, '2024-11-24', '2024-11-24 16:00:00', 300.00, 'MacBook Pro', 'high_risk_region1', 200.00, 5, false, NULL),
            (1005, 5005, '2024-11-24', '2024-11-24 11:00:00', 1100.00, 'iPad Pro', 'Chicago', 200.00, 10, false, NULL);
        """
        sql """
           UPDATE transaction_details
           SET risk_level = CASE
             -- Transactions with dispute history or in high-risk regions
             WHEN has_dispute_history = TRUE THEN 'high'
             WHEN transaction_region IN ('high_risk_region1', 'high_risk_region2') THEN 'high'

             -- Abnormal transaction amount
             WHEN transaction_amount > 5 * average_daily_amount THEN 'high'

             -- High transaction frequency in the last 7 days
             WHEN recent_transaction_count > 50 THEN 'high'
             WHEN recent_transaction_count BETWEEN 20 AND 50 THEN 'medium'

             -- Transactions during non-working hours
             WHEN HOUR(transaction_time) BETWEEN 2 AND 4 THEN 'medium'

             -- Default risk
             ELSE 'low'
           END
           WHERE transaction_date = '2024-11-24';
        """
        qt_sql "SELECT * FROM transaction_details order by transaction_id"
    } catch (Throwable t) {
        Assertions.fail("examples in docs/data-operate/update/unique-update.md failed to exec, please fix it", t)
    }
}
