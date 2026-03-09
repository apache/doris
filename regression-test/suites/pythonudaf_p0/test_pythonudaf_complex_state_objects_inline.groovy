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

suite("test_pythonudaf_complex_state_objects_inline") {
    // Comprehensive test for complex Python objects as aggregate states
    // Tests various pickle-serializable data structures:
    // 1. Nested dictionaries
    // 2. Custom classes (dataclass)
    // 3. Lists of tuples
    // 4. Sets and frozensets
    // 5. Named tuples
    // 6. Mixed complex structures
    
    def runtime_version = "3.8.10"
    
    try {
        // ========================================
        // Setup: Create test tables
        // ========================================
        
        // Table 1: Transaction data for complex aggregations
        sql """ DROP TABLE IF EXISTS complex_transactions; """
        sql """
        CREATE TABLE complex_transactions (
            transaction_id INT,
            user_id INT,
            product_id INT,
            product_name VARCHAR(100),
            category VARCHAR(50),
            price DECIMAL(10,2),
            quantity INT,
            timestamp DATETIME,
            region VARCHAR(50),
            payment_method VARCHAR(50)
        ) ENGINE=OLAP 
        DUPLICATE KEY(transaction_id)
        DISTRIBUTED BY HASH(transaction_id) BUCKETS 3
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO complex_transactions VALUES
        (1, 101, 1001, 'Laptop Pro', 'Electronics', 1299.99, 1, '2024-01-01 10:00:00', 'North', 'Credit'),
        (2, 101, 1002, 'Mouse', 'Electronics', 29.99, 2, '2024-01-01 10:05:00', 'North', 'Credit'),
        (3, 102, 1003, 'Keyboard', 'Electronics', 79.99, 1, '2024-01-02 11:00:00', 'South', 'Debit'),
        (4, 101, 1004, 'Monitor', 'Electronics', 399.99, 1, '2024-01-03 09:30:00', 'North', 'Credit'),
        (5, 103, 1001, 'Laptop Pro', 'Electronics', 1299.99, 1, '2024-01-03 14:00:00', 'East', 'PayPal'),
        (6, 102, 1005, 'USB Cable', 'Accessories', 9.99, 3, '2024-01-04 10:00:00', 'South', 'Cash'),
        (7, 104, 1002, 'Mouse', 'Electronics', 29.99, 1, '2024-01-04 15:00:00', 'West', 'Credit'),
        (8, 103, 1006, 'Webcam', 'Electronics', 89.99, 1, '2024-01-05 11:00:00', 'East', 'PayPal'),
        (9, 105, 1003, 'Keyboard', 'Electronics', 79.99, 2, '2024-01-05 16:00:00', 'North', 'Debit'),
        (10, 104, 1007, 'HDMI Cable', 'Accessories', 15.99, 2, '2024-01-06 10:00:00', 'West', 'Cash'),
        (11, 101, 1008, 'Headphones', 'Electronics', 149.99, 1, '2024-01-06 14:00:00', 'North', 'Credit'),
        (12, 106, 1004, 'Monitor', 'Electronics', 399.99, 2, '2024-01-07 09:00:00', 'South', 'Credit'),
        (13, 102, 1009, 'Desk Lamp', 'Home', 45.99, 1, '2024-01-07 15:00:00', 'South', 'Debit'),
        (14, 107, 1010, 'Office Chair', 'Furniture', 299.99, 1, '2024-01-08 10:00:00', 'East', 'Credit'),
        (15, 103, 1002, 'Mouse', 'Electronics', 29.99, 3, '2024-01-08 11:00:00', 'East', 'PayPal');
        """
        
        // ========================================
        // UDAF 1: Nested Dictionary State - User Purchase Profile
        // Tracks: {user_id: {'total_spent': float, 'items': [product_names], 'categories': set}}
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_user_profile(INT, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_user_profile(INT, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "UserProfileUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
class UserProfileUDAF:
    def __init__(self):
        # Complex nested structure: dict of dicts with lists and sets
        self.profiles = {}
    
    @property
    def aggregate_state(self):
        # Convert sets to lists for pickle serialization
        serializable = {}
        for user_id, profile in self.profiles.items():
            serializable[user_id] = {
                'total_spent': profile['total_spent'],
                'items': profile['items'],
                'categories': list(profile['categories'])
            }
        return serializable
    
    def accumulate(self, user_id, product_name, category, price, quantity):
        if user_id is None:
            return
        
        if user_id not in self.profiles:
            self.profiles[user_id] = {
                'total_spent': 0.0,
                'items': [],
                'categories': set()
            }
        
        revenue = float(price) * int(quantity) if price and quantity else 0.0
        self.profiles[user_id]['total_spent'] += revenue
        if product_name:
            self.profiles[user_id]['items'].append(product_name)
        if category:
            self.profiles[user_id]['categories'].add(category)
    
    def merge(self, other_state):
        for user_id, profile in other_state.items():
            if user_id not in self.profiles:
                self.profiles[user_id] = {
                    'total_spent': 0.0,
                    'items': [],
                    'categories': set()
                }
            
            self.profiles[user_id]['total_spent'] += profile['total_spent']
            self.profiles[user_id]['items'].extend(profile['items'])
            self.profiles[user_id]['categories'].update(profile['categories'])
    
    def finish(self):
        # Return summary as JSON string
        import json
        result = {}
        for user_id, profile in self.profiles.items():
            result[str(user_id)] = {
                'total_spent': round(profile['total_spent'], 2),
                'item_count': len(profile['items']),
                'unique_categories': len(profile['categories'])
            }
        return json.dumps(result, sort_keys=True)
\$\$;
        """
        
        // ========================================
        // UDAF 2: Custom Class State - Product Statistics
        // Uses a custom StatisticsTracker class
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_product_stats(VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_product_stats(VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "ProductStatsUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
from dataclasses import dataclass
from typing import List
import json

@dataclass
class ProductStats:
    product_name: str
    prices: List[float]
    quantities: List[int]
    
    def total_revenue(self):
        return sum(p * q for p, q in zip(self.prices, self.quantities))
    
    def avg_price(self):
        return sum(self.prices) / len(self.prices) if self.prices else 0.0
    
    def total_quantity(self):
        return sum(self.quantities)

class ProductStatsUDAF:
    def __init__(self):
        self.stats = {}  # product_name -> ProductStats
    
    @property
    def aggregate_state(self):
        # Convert dataclass instances to dicts for serialization
        return {
            name: {
                'product_name': stat.product_name,
                'prices': stat.prices,
                'quantities': stat.quantities
            }
            for name, stat in self.stats.items()
        }
    
    def accumulate(self, product_name, price, quantity):
        if product_name is None:
            return
        
        if product_name not in self.stats:
            self.stats[product_name] = ProductStats(
                product_name=product_name,
                prices=[],
                quantities=[]
            )
        
        if price is not None:
            self.stats[product_name].prices.append(float(price))
        if quantity is not None:
            self.stats[product_name].quantities.append(int(quantity))
    
    def merge(self, other_state):
        for name, stat_dict in other_state.items():
            if name not in self.stats:
                self.stats[name] = ProductStats(
                    product_name=stat_dict['product_name'],
                    prices=stat_dict['prices'][:],
                    quantities=stat_dict['quantities'][:]
                )
            else:
                self.stats[name].prices.extend(stat_dict['prices'])
                self.stats[name].quantities.extend(stat_dict['quantities'])
    
    def finish(self):
        result = {}
        for name, stat in self.stats.items():
            result[name] = {
                'avg_price': round(stat.avg_price(), 2),
                'total_quantity': stat.total_quantity(),
                'total_revenue': round(stat.total_revenue(), 2),
                'transactions': len(stat.prices)
            }
        return json.dumps(result, sort_keys=True)
\$\$;
        """
        
        // ========================================
        // UDAF 3: List of Tuples State - Transaction Timeline
        // Stores chronological list of (timestamp, amount) tuples
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_transaction_timeline(DATETIME, DECIMAL); """
        sql """
        CREATE AGGREGATE FUNCTION py_transaction_timeline(DATETIME, DECIMAL)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "TransactionTimelineUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import json
from datetime import datetime

class TransactionTimelineUDAF:
    def __init__(self):
        # List of tuples: [(timestamp_str, amount), ...]
        self.timeline = []
    
    @property
    def aggregate_state(self):
        return self.timeline
    
    def accumulate(self, timestamp, amount):
        if timestamp is not None and amount is not None:
            # Convert datetime to string for serialization
            ts_str = str(timestamp)
            self.timeline.append((ts_str, float(amount)))
    
    def merge(self, other_state):
        self.timeline.extend(other_state)
    
    def finish(self):
        # Sort by timestamp and return summary
        sorted_timeline = sorted(self.timeline, key=lambda x: x[0])
        
        if not sorted_timeline:
            return json.dumps({'count': 0})
        
        total = sum(amount for _, amount in sorted_timeline)
        
        result = {
            'count': len(sorted_timeline),
            'total': round(total, 2),
            'first_transaction': sorted_timeline[0][0],
            'last_transaction': sorted_timeline[-1][0],
            'first_amount': round(sorted_timeline[0][1], 2),
            'last_amount': round(sorted_timeline[-1][1], 2)
        }
        return json.dumps(result)
\$\$;
        """
        
        // ========================================
        // UDAF 4: Set-based State - Unique Value Tracker
        // Tracks unique users, products, and payment methods
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_unique_tracker(INT, INT, VARCHAR); """
        sql """
        CREATE AGGREGATE FUNCTION py_unique_tracker(INT, INT, VARCHAR)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "UniqueTrackerUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import json

class UniqueTrackerUDAF:
    def __init__(self):
        # Use sets to track unique values
        self.unique_users = set()
        self.unique_products = set()
        self.payment_methods = set()
    
    @property
    def aggregate_state(self):
        # Convert sets to lists for pickle
        return {
            'users': list(self.unique_users),
            'products': list(self.unique_products),
            'payments': list(self.payment_methods)
        }
    
    def accumulate(self, user_id, product_id, payment_method):
        if user_id is not None:
            self.unique_users.add(user_id)
        if product_id is not None:
            self.unique_products.add(product_id)
        if payment_method is not None:
            self.payment_methods.add(payment_method)
    
    def merge(self, other_state):
        self.unique_users.update(other_state['users'])
        self.unique_products.update(other_state['products'])
        self.payment_methods.update(other_state['payments'])
    
    def finish(self):
        return json.dumps({
            'unique_users': len(self.unique_users),
            'unique_products': len(self.unique_products),
            'payment_methods': sorted(list(self.payment_methods))
        })
\$\$;
        """
        
        // ========================================
        // UDAF 5: Named Tuple State - Category Summary
        // Uses collections.namedtuple for structured data
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_category_summary(VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_category_summary(VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "CategorySummaryUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
from collections import namedtuple
import json

CategoryData = namedtuple('CategoryData', ['total_revenue', 'total_items', 'transaction_count'])

class CategorySummaryUDAF:
    def __init__(self):
        # Dict of category -> namedtuple
        self.categories = {}
    
    @property
    def aggregate_state(self):
        # Convert namedtuples to tuples for pickle
        return {
            cat: (data.total_revenue, data.total_items, data.transaction_count)
            for cat, data in self.categories.items()
        }
    
    def accumulate(self, category, price, quantity):
        if category is None:
            return
        
        revenue = float(price) * int(quantity) if price and quantity else 0.0
        items = int(quantity) if quantity else 0
        
        if category in self.categories:
            old = self.categories[category]
            self.categories[category] = CategoryData(
                total_revenue=old.total_revenue + revenue,
                total_items=old.total_items + items,
                transaction_count=old.transaction_count + 1
            )
        else:
            self.categories[category] = CategoryData(
                total_revenue=revenue,
                total_items=items,
                transaction_count=1
            )
    
    def merge(self, other_state):
        for cat, (revenue, items, count) in other_state.items():
            if cat in self.categories:
                old = self.categories[cat]
                self.categories[cat] = CategoryData(
                    total_revenue=old.total_revenue + revenue,
                    total_items=old.total_items + items,
                    transaction_count=old.transaction_count + count
                )
            else:
                self.categories[cat] = CategoryData(
                    total_revenue=revenue,
                    total_items=items,
                    transaction_count=count
                )
    
    def finish(self):
        result = {}
        for cat, data in self.categories.items():
            result[cat] = {
                'total_revenue': round(data.total_revenue, 2),
                'total_items': data.total_items,
                'transactions': data.transaction_count,
                'avg_per_transaction': round(data.total_revenue / data.transaction_count, 2) if data.transaction_count > 0 else 0.0
            }
        return json.dumps(result, sort_keys=True)
\$\$;
        """
        
        // ========================================
        // UDAF 6: Complex Nested State - Hierarchical Aggregation
        // Multi-level nested structure: region -> category -> product -> stats
        // ========================================
        sql """ DROP FUNCTION IF EXISTS py_hierarchical_agg(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """
        CREATE AGGREGATE FUNCTION py_hierarchical_agg(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT)
        RETURNS VARCHAR
        PROPERTIES (
            "type" = "PYTHON_UDF",
            "symbol" = "HierarchicalAggUDAF",
            "runtime_version" = "${runtime_version}"
        )
        AS \$\$
import json
from collections import defaultdict

class HierarchicalAggUDAF:
    def __init__(self):
        # Complex nested dict: {region: {category: {product: {'revenue': float, 'quantity': int}}}}
        self.hierarchy = {}
    
    @property
    def aggregate_state(self):
        return self.hierarchy
    
    def accumulate(self, region, category, product, price, quantity):
        if not all([region, category, product]):
            return
        
        if region not in self.hierarchy:
            self.hierarchy[region] = {}
        if category not in self.hierarchy[region]:
            self.hierarchy[region][category] = {}
        if product not in self.hierarchy[region][category]:
            self.hierarchy[region][category][product] = {'revenue': 0.0, 'quantity': 0}
        
        revenue = float(price) * int(quantity) if price and quantity else 0.0
        qty = int(quantity) if quantity else 0
        
        self.hierarchy[region][category][product]['revenue'] += revenue
        self.hierarchy[region][category][product]['quantity'] += qty
    
    def merge(self, other_state):
        for region, categories in other_state.items():
            if region not in self.hierarchy:
                self.hierarchy[region] = {}
            
            for category, products in categories.items():
                if category not in self.hierarchy[region]:
                    self.hierarchy[region][category] = {}
                
                for product, stats in products.items():
                    if product not in self.hierarchy[region][category]:
                        self.hierarchy[region][category][product] = {'revenue': 0.0, 'quantity': 0}
                    
                    self.hierarchy[region][category][product]['revenue'] += stats['revenue']
                    self.hierarchy[region][category][product]['quantity'] += stats['quantity']
    
    def finish(self):
        # Summarize hierarchy at each level
        result = {}
        for region, categories in self.hierarchy.items():
            region_total = 0.0
            region_data = {}
            
            for category, products in categories.items():
                category_total = sum(p['revenue'] for p in products.values())
                region_total += category_total
                region_data[category] = {
                    'revenue': round(category_total, 2),
                    'products': len(products)
                }
            
            result[region] = {
                'total_revenue': round(region_total, 2),
                'categories': region_data
            }
        
        return json.dumps(result, sort_keys=True)
\$\$;
        """
        
        // ========================================
        // Test Cases
        // ========================================
        
        // Test 1: User Profile Aggregation (Nested Dict)
        qt_test_user_profile """
            SELECT 
                py_user_profile(user_id, product_name, category, price, quantity) as user_profiles
            FROM complex_transactions;
        """
        
        // Test 2: Product Statistics (Custom Class)
        qt_test_product_stats """
            SELECT 
                py_product_stats(product_name, price, quantity) as product_statistics
            FROM complex_transactions;
        """
        
        // Test 3: Transaction Timeline (List of Tuples)
        qt_test_transaction_timeline """
            SELECT 
                region,
                py_transaction_timeline(timestamp, price * quantity) as timeline
            FROM complex_transactions
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 4: Unique Tracker (Sets)
        qt_test_unique_tracker """
            SELECT 
                category,
                py_unique_tracker(user_id, product_id, payment_method) as unique_stats
            FROM complex_transactions
            GROUP BY category
            ORDER BY category;
        """
        
        // Test 5: Category Summary (Named Tuples)
        qt_test_category_summary """
            SELECT 
                py_category_summary(category, price, quantity) as category_summary
            FROM complex_transactions;
        """
        
        // Test 6: Hierarchical Aggregation (Deep Nesting)
        qt_test_hierarchical_agg """
            SELECT 
                py_hierarchical_agg(region, category, product_name, price, quantity) as hierarchy
            FROM complex_transactions;
        """
        
        // Test 7: Complex State with Window Function
        qt_test_complex_window """
            SELECT 
                user_id,
                product_name,
                price,
                py_user_profile(user_id, product_name, category, price, quantity) 
                    OVER (PARTITION BY user_id ORDER BY transaction_id) as running_profile
            FROM complex_transactions
            ORDER BY user_id, transaction_id;
        """
        
        // Test 8: Multiple Complex UDAFs in Single Query
        qt_test_multi_complex """
            SELECT 
                region,
                py_unique_tracker(user_id, product_id, payment_method) as uniques,
                py_category_summary(category, price, quantity) as summary
            FROM complex_transactions
            GROUP BY region
            ORDER BY region;
        """
        
        // Test 9: Nested Query with Complex State
        qt_test_nested_complex """
            SELECT 
                region,
                product_stats
            FROM (
                SELECT 
                    region,
                    py_product_stats(product_name, price, quantity) as product_stats
                FROM complex_transactions
                WHERE price > 50
                GROUP BY region
            ) t
            ORDER BY region;
        """
        
        // Test 10: Complex State Serialization in Shuffle (GROUP BY multiple columns)
        qt_test_complex_shuffle """
            SELECT 
                region,
                category,
                py_hierarchical_agg(region, category, product_name, price, quantity) as hier_stats
            FROM complex_transactions
            GROUP BY region, category
            ORDER BY region, category;
        """
        
        // Test 11: Edge Case - Empty Groups
        qt_test_empty_groups """
            SELECT 
                region,
                py_user_profile(user_id, product_name, category, price, quantity) as profile
            FROM complex_transactions
            WHERE 1 = 0
            GROUP BY region;
        """
        
        // Test 12: Edge Case - NULL Values
        sql """ DROP TABLE IF EXISTS complex_nulls; """
        sql """
        CREATE TABLE complex_nulls (
            id INT,
            user_id INT,
            product VARCHAR(50),
            category VARCHAR(50),
            price DECIMAL(10,2)
        ) ENGINE=OLAP 
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1");
        """
        
        sql """
        INSERT INTO complex_nulls VALUES
        (1, 101, 'ItemA', 'Cat1', 100.0),
        (2, NULL, 'ItemB', 'Cat2', 200.0),
        (3, 102, NULL, 'Cat3', 300.0),
        (4, 103, 'ItemC', NULL, 400.0),
        (5, 104, 'ItemD', 'Cat4', NULL);
        """
        
        qt_test_null_handling """
            SELECT 
                py_user_profile(user_id, product, category, price, 1) as profile_with_nulls
            FROM complex_nulls;
        """
        
        // Test 13: Performance - Large Complex State
        qt_test_large_state """
            SELECT 
                COUNT(*) as total_transactions,
                py_hierarchical_agg(region, category, product_name, price, quantity) as full_hierarchy,
                py_user_profile(user_id, product_name, category, price, quantity) as all_profiles
            FROM complex_transactions;
        """
        
    } finally {
        // Cleanup
        sql """ DROP TABLE IF EXISTS complex_transactions; """
        sql """ DROP TABLE IF EXISTS complex_nulls; """
        
        sql """ DROP FUNCTION IF EXISTS py_user_profile(INT, VARCHAR, VARCHAR, DECIMAL, INT); """
        sql """ DROP FUNCTION IF EXISTS py_product_stats(VARCHAR, DECIMAL, INT); """
        sql """ DROP FUNCTION IF EXISTS py_transaction_timeline(DATETIME, DECIMAL); """
        sql """ DROP FUNCTION IF EXISTS py_unique_tracker(INT, INT, VARCHAR); """
        sql """ DROP FUNCTION IF EXISTS py_category_summary(VARCHAR, DECIMAL, INT); """
        sql """ DROP FUNCTION IF EXISTS py_hierarchical_agg(VARCHAR, VARCHAR, VARCHAR, DECIMAL, INT); """
    }
}
