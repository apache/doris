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

suite("test_pythonudf_multiline_inline") {
    // Test complex multi-line inline Python code
    
    def runtime_version = "3.8.10"
    try {
        // Test 1: Inline code with helper functions
        sql """ DROP FUNCTION IF EXISTS py_complex_calculation(INT, INT); """
        sql """
        CREATE FUNCTION py_complex_calculation(INT, INT) 
        RETURNS INT 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def helper_function(x):
    return x * x

def evaluate(a, b):
    if a is None or b is None:
        return None
    result = helper_function(a) + helper_function(b)
    return result
\$\$;
        """
        
        qt_select_complex_calc """ SELECT py_complex_calculation(3, 4) AS result; """
        
        // Test 2: Complex function with conditional logic
        sql """ DROP FUNCTION IF EXISTS py_business_logic(STRING, DOUBLE, INT); """
        sql """
        CREATE FUNCTION py_business_logic(STRING, DOUBLE, INT) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(customer_type, amount, quantity):
    if customer_type is None or amount is None or quantity is None:
        return 'INVALID'
    
    # Calculate discount
    discount = 0
    if customer_type == 'VIP':
        discount = 0.2
    elif customer_type == 'PREMIUM':
        discount = 0.15
    elif customer_type == 'REGULAR':
        discount = 0.1
    else:
        discount = 0
    
    # Bulk discount
    if quantity >= 100:
        discount += 0.05
    elif quantity >= 50:
        discount += 0.03
    
    # Calculate final price
    final_amount = amount * (1 - discount)
    
    # Return result
    if final_amount > 10000:
        return f'HIGH:{final_amount:.2f}'
    elif final_amount > 1000:
        return f'MEDIUM:{final_amount:.2f}'
    else:
        return f'LOW:{final_amount:.2f}'
\$\$;
        """
        
        qt_select_business_logic_vip """ SELECT py_business_logic('VIP', 5000.0, 120) AS result; """
        qt_select_business_logic_regular """ SELECT py_business_logic('REGULAR', 2000.0, 30) AS result; """
        
        // Test 3: Complex string processing logic
        sql """ DROP FUNCTION IF EXISTS py_text_analyzer(STRING); """
        sql """
        CREATE FUNCTION py_text_analyzer(STRING) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(text):
    if text is None:
        return None
    
    # Collect statistics
    length = len(text)
    words = text.split()
    word_count = len(words)
    
    # Count character types
    upper_count = sum(1 for c in text if c.isupper())
    lower_count = sum(1 for c in text if c.islower())
    digit_count = sum(1 for c in text if c.isdigit())
    
    # Build result
    result = f"len:{length},words:{word_count},upper:{upper_count},lower:{lower_count},digits:{digit_count}"
    return result
\$\$;
        """
        
        qt_select_text_analyzer """ SELECT py_text_analyzer('Hello World 123') AS result; """
        
        // Test 4: Complex mathematical calculation function
        sql """ DROP FUNCTION IF EXISTS py_statistics(DOUBLE, DOUBLE, DOUBLE, DOUBLE); """
        sql """
        CREATE FUNCTION py_statistics(DOUBLE, DOUBLE, DOUBLE, DOUBLE) 
        RETURNS STRING 
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "evaluate",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
def evaluate(v1, v2, v3, v4):
    if any(x is None for x in [v1, v2, v3, v4]):
        return None
    
    values = [v1, v2, v3, v4]
    
    # Calculate statistics
    total = sum(values)
    count = len(values)
    mean = total / count
    
    # Calculate variance
    variance = sum((x - mean) ** 2 for x in values) / count
    
    # Calculate standard deviation
    import math
    std_dev = math.sqrt(variance)
    
    # Find max and min values
    max_val = max(values)
    min_val = min(values)
    
    result = f"mean:{mean:.2f},std:{std_dev:.2f},max:{max_val:.2f},min:{min_val:.2f}"
    return result
\$\$;
        """
        
        qt_select_statistics """ SELECT py_statistics(10.0, 20.0, 30.0, 40.0) AS result; """
        
        // Test 5: Test complex inline code on table data
        sql """ DROP TABLE IF EXISTS multiline_test_table; """
        sql """
        CREATE TABLE multiline_test_table (
            id INT,
            customer_type STRING,
            amount DOUBLE,
            quantity INT,
            description STRING
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO multiline_test_table VALUES
        (1, 'VIP', 15000.0, 150, 'Premium customer order'),
        (2, 'PREMIUM', 8000.0, 80, 'Good customer'),
        (3, 'REGULAR', 3000.0, 40, 'Regular order'),
        (4, 'VIP', 500.0, 10, 'Small VIP order'),
        (5, 'REGULAR', 12000.0, 200, 'Large regular order');
        """
        
        qt_select_table_multiline """ 
        SELECT 
            id,
            customer_type,
            amount,
            quantity,
            py_business_logic(customer_type, amount, quantity) AS pricing_result,
            py_text_analyzer(description) AS text_analysis
        FROM multiline_test_table
        ORDER BY id;
        """
        
    } finally {
        try_sql("DROP FUNCTION IF EXISTS py_complex_calculation(INT, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_business_logic(STRING, DOUBLE, INT);")
        try_sql("DROP FUNCTION IF EXISTS py_text_analyzer(STRING);")
        try_sql("DROP FUNCTION IF EXISTS py_statistics(DOUBLE, DOUBLE, DOUBLE, DOUBLE);")
        try_sql("DROP TABLE IF EXISTS multiline_test_table;")
    }
}
