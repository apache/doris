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

suite("test_pythonudf_mixed_params") {
    // Test vectorized Python UDF with mixed parameter types (pd.Series + scalar)
    // This tests the scenario where some parameters are vectorized (pd.Series) 
    // and some are scalar values (int, float, str)
    // 
    // Key concept: In vectorized UDF, you can mix:
    //   - pd.Series parameters (process entire column)
    //   - scalar parameters (single value like int, float, str)
    
    def runtime_version = "3.8.10"
    
    try {
        // Create test table
        sql """ DROP TABLE IF EXISTS test_mixed_params_table; """
        sql """
        CREATE TABLE test_mixed_params_table (
            id INT,
            price DOUBLE,
            quantity INT,
            discount_rate DOUBLE,
            category STRING
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        // Insert test data
        sql """
        INSERT INTO test_mixed_params_table VALUES
        (1, 100.0, 5, 0.1, 'A'),
        (2, 200.0, 3, 0.15, 'B'),
        (3, 150.0, 8, 0.2, 'A'),
        (4, 300.0, 2, 0.05, 'C'),
        (5, 250.0, 6, 0.12, 'B'),
        (6, 180.0, 4, 0.18, 'A'),
        (7, 220.0, 7, 0.08, 'C'),
        (8, 120.0, 9, 0.25, 'B'),
        (9, 280.0, 1, 0.1, 'A'),
        (10, 350.0, 5, 0.15, 'C');
        """
        
        sql "sync"
        
        // ==================== Test 1: pd.Series + scalar float ====================
        log.info("=== Test 1: pd.Series + scalar float ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply_constant(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_multiply_constant(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_multiply_constant",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_multiply_constant(values: pd.Series, multiplier: float) -> pd.Series:
    # values: pd.Series (vectorized column data)
    # multiplier: float (scalar constant)
    return values * multiplier
\$\$;
        """
        
        qt_select_1 """
        SELECT 
            id,
            price,
            py_vec_multiply_constant(price, 1.5) AS price_multiplied
        FROM test_mixed_params_table
        ORDER BY id;
        """
        
        // ==================== Test 2: Multiple pd.Series + scalar float ====================
        log.info("=== Test 2: Multiple pd.Series + scalar float ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_calc_total(DOUBLE, INT, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_calc_total(DOUBLE, INT, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_calc_total",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_calc_total(price: pd.Series, quantity: pd.Series, tax_rate: float) -> pd.Series:
    # price: pd.Series (vectorized)
    # quantity: pd.Series (vectorized)
    # tax_rate: float (scalar constant)
    subtotal = price * quantity
    return subtotal * (1 + tax_rate)
\$\$;
        """
        
        qt_select_2 """
        SELECT 
            id,
            price,
            quantity,
            py_vec_calc_total(price, quantity, 0.1) AS total_with_tax
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 3: Two pd.Series (both vectorized) ====================
        log.info("=== Test 3: Two pd.Series (both vectorized) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_apply_discount(DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_apply_discount(DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_apply_discount",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_apply_discount(price: pd.Series, discount_rate: pd.Series) -> pd.Series:
    # Both are pd.Series (vectorized)
    # Each row has its own discount rate from the column
    return price * (1 - discount_rate)
\$\$;
        """
        
        qt_select_3 """
        SELECT 
            id,
            price,
            discount_rate,
            py_vec_apply_discount(price, discount_rate) AS final_price
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 4: Complex Mixed Parameters (3 Series + 1 scalar) ====================
        log.info("=== Test 4: Complex calculation with mixed params (3 Series + 1 scalar) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_complex_calc(DOUBLE, INT, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_complex_calc(DOUBLE, INT, DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_complex_calc",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_complex_calc(price: pd.Series, quantity: pd.Series, discount_rate: pd.Series, shipping_fee: float) -> pd.Series:
    # price: pd.Series (vectorized)
    # quantity: pd.Series (vectorized)
    # discount_rate: pd.Series (vectorized, per-row discount)
    # shipping_fee: float (scalar constant)
    
    # Calculate: (price * quantity) * (1 - discount) + shipping_fee
    subtotal = price * quantity
    after_discount = subtotal * (1 - discount_rate)
    return after_discount + shipping_fee
\$\$;
        """
        
        qt_select_4 """
        SELECT 
            id,
            price,
            quantity,
            discount_rate,
            py_vec_complex_calc(price, quantity, discount_rate, 10.0) AS final_total
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 5: String pd.Series + scalar str ====================
        log.info("=== Test 5: String pd.Series + scalar str ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_add_prefix(STRING, STRING); """
        sql """
        CREATE FUNCTION py_vec_add_prefix(STRING, STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_add_prefix",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_add_prefix(categories: pd.Series, prefix: str) -> pd.Series:
    # categories: pd.Series (vectorized string column)
    # prefix: str (scalar constant)
    return prefix + '_' + categories
\$\$;
        """
        
        qt_select_5 """
        SELECT 
            id,
            category,
            py_vec_add_prefix(category, 'CAT') AS prefixed_category
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 6: pd.Series + scalar int ====================
        log.info("=== Test 6: pd.Series + scalar int ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_add_int(INT, INT); """
        sql """
        CREATE FUNCTION py_vec_add_int(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_add_int",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_add_int(quantities: pd.Series, bonus: int) -> pd.Series:
    # quantities: pd.Series (vectorized int column)
    # bonus: int (scalar constant)
    return quantities + bonus
\$\$;
        """
        
        qt_select_6 """
        SELECT 
            id,
            quantity,
            py_vec_add_int(quantity, 10) AS quantity_with_bonus
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 7: Conditional Logic with Mixed Params ====================
        log.info("=== Test 7: Conditional logic with mixed params (2 Series + 1 scalar) ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_conditional_discount(DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_conditional_discount(DOUBLE, DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_conditional_discount",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd
import numpy as np

def py_vec_conditional_discount(price: pd.Series, discount_rate: pd.Series, threshold: float) -> pd.Series:
    # price: pd.Series (vectorized)
    # discount_rate: pd.Series (vectorized)
    # threshold: float (scalar constant - minimum price for discount)
    
    # Apply discount only if price >= threshold
    result = np.where(price >= threshold, 
                     price * (1 - discount_rate),
                     price)
    return pd.Series(result)
\$\$;
        """
        
        qt_select_7 """
        SELECT 
            id,
            price,
            discount_rate,
            py_vec_conditional_discount(price, discount_rate, 200.0) AS final_price
        FROM test_mixed_params_table
        ORDER BY id;
        """
        
        // ==================== Test 8: Scalar first, then Series ====================
        log.info("=== Test 8: Scalar parameter first, then Series ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_scale_and_add(DOUBLE, DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_vec_scale_and_add(DOUBLE, DOUBLE, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_scale_and_add",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_scale_and_add(scale_factor: float, prices: pd.Series, quantities: pd.Series) -> pd.Series:
    # scale_factor: float (scalar constant)
    # prices: pd.Series (vectorized)
    # quantities: pd.Series (vectorized)
    return (prices * quantities) * scale_factor
\$\$;
        """
        
        qt_select_8 """
        SELECT 
            id,
            price,
            quantity,
            py_vec_scale_and_add(1.2, price, quantity) AS scaled_total
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 3;
        """
        
        // ==================== Test 9: Alternating Series and Scalar ====================
        log.info("=== Test 9: Alternating Series and scalar parameters ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_alternating(DOUBLE, DOUBLE, INT, INT); """
        sql """
        CREATE FUNCTION py_vec_alternating(DOUBLE, DOUBLE, INT, INT) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_alternating",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_alternating(prices: pd.Series, markup: float, quantities: pd.Series, min_qty: int) -> pd.Series:
    # prices: pd.Series (vectorized)
    # markup: float (scalar constant)
    # quantities: pd.Series (vectorized)
    # min_qty: int (scalar constant)
    
    import numpy as np
    # Apply markup only if quantity >= min_qty
    result = np.where(quantities >= min_qty,
                     prices * (1 + markup),
                     prices)
    return pd.Series(result)
\$\$;
        """
        
        qt_select_9 """
        SELECT 
            id,
            price,
            quantity,
            py_vec_alternating(price, 0.2, quantity, 5) AS conditional_price
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 5;
        """
        
        // ==================== Test 10: Multiple scalars with one Series ====================
        log.info("=== Test 10: Multiple scalar parameters with one Series ===")
        
        sql """ DROP FUNCTION IF EXISTS py_vec_multi_scalar(DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_vec_multi_scalar(DOUBLE, DOUBLE, DOUBLE, DOUBLE) 
        RETURNS DOUBLE 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "py_vec_multi_scalar",
            "runtime_version" = "${runtime_version}",
            "always_nullable" = "true"
        )
        AS \$\$
import pandas as pd

def py_vec_multi_scalar(prices: pd.Series, tax: float, discount: float, fee: float) -> pd.Series:
    # prices: pd.Series (vectorized)
    # tax: float (scalar constant)
    # discount: float (scalar constant)
    # fee: float (scalar constant)
    
    # Calculate: (price * (1 - discount)) * (1 + tax) + fee
    after_discount = prices * (1 - discount)
    with_tax = after_discount * (1 + tax)
    return with_tax + fee
\$\$;
        """
        
        qt_select_10 """
        SELECT 
            id,
            price,
            py_vec_multi_scalar(price, 0.1, 0.05, 5.0) AS final_price
        FROM test_mixed_params_table
        ORDER BY id
        LIMIT 3;
        """
        
        log.info("All mixed parameter tests passed!")
        
    } finally {
        // Cleanup
        sql """ DROP FUNCTION IF EXISTS py_vec_multiply_constant(DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_calc_total(DOUBLE, INT, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_apply_discount(DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_complex_calc(DOUBLE, INT, DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_add_prefix(STRING, STRING); """
        sql """ DROP FUNCTION IF EXISTS py_vec_add_int(INT, INT); """
        sql """ DROP FUNCTION IF EXISTS py_vec_conditional_discount(DOUBLE, DOUBLE, DOUBLE); """
        sql """ DROP FUNCTION IF EXISTS py_vec_scale_and_add(DOUBLE, DOUBLE, INT); """
        sql """ DROP FUNCTION IF EXISTS py_vec_alternating(DOUBLE, DOUBLE, INT, INT); """
        sql """ DROP FUNCTION IF EXISTS py_vec_multi_scalar(DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """ DROP TABLE IF EXISTS test_mixed_params_table; """
    }
}
