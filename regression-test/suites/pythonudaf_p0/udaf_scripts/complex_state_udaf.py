#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Complex State Object UDAFs for Doris Python UDAF
Tests various pickle-serializable data structures
"""

import json
from dataclasses import dataclass
from typing import List
from collections import namedtuple, deque
from datetime import datetime


# ========================================
# UDAF 1: Nested Dictionary State - User Purchase Profile
# ========================================
class UserProfileUDAF:
    """Tracks user purchase profiles with nested dict structure"""
    
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
        result = {}
        for user_id, profile in self.profiles.items():
            result[str(user_id)] = {
                'total_spent': round(profile['total_spent'], 2),
                'item_count': len(profile['items']),
                'unique_categories': len(profile['categories'])
            }
        return json.dumps(result, sort_keys=True)


# ========================================
# UDAF 2: Custom Class State - Product Statistics
# ========================================
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
    """Product statistics using dataclass"""
    
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


# ========================================
# UDAF 3: List of Tuples State - Transaction Timeline
# ========================================
class TransactionTimelineUDAF:
    """Stores chronological list of (timestamp, amount) tuples"""
    
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


# ========================================
# UDAF 4: Set-based State - Unique Value Tracker
# ========================================
class UniqueTrackerUDAF:
    """Tracks unique users, products, and payment methods using sets"""
    
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


# ========================================
# UDAF 5: Named Tuple State - Category Summary
# ========================================
CategoryData = namedtuple('CategoryData', ['total_revenue', 'total_items', 'transaction_count'])


class CategorySummaryUDAF:
    """Uses collections.namedtuple for structured data"""
    
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


# ========================================
# UDAF 6: Complex Nested State - Hierarchical Aggregation
# ========================================
class HierarchicalAggUDAF:
    """Multi-level nested structure: region -> category -> product -> stats"""
    
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


# ========================================
# UDAF 7: Deque-based State - Recent Transactions Window
# Modified to use sorted aggregation for deterministic results
# ========================================
class RecentWindowUDAF:
    """Aggregates transactions with deterministic sorting"""
    
    def __init__(self):
        # Keep all transactions for deterministic ordering
        self.all_transactions = []
    
    @property
    def aggregate_state(self):
        # Return all transactions for merging
        return self.all_transactions
    
    def accumulate(self, price, quantity):
        if price is not None and quantity is not None:
            revenue = float(price) * int(quantity)
            self.all_transactions.append(revenue)
    
    def merge(self, other_state):
        # Merge all transactions
        self.all_transactions.extend(other_state)
    
    def finish(self):
        if not self.all_transactions:
            return json.dumps({'count': 0})
        
        # Sort for deterministic results, then take last 5
        sorted_trans = sorted(self.all_transactions)
        window = sorted_trans[-5:] if len(sorted_trans) > 5 else sorted_trans
        
        return json.dumps({
            'count': len(window),
            'values': [round(v, 2) for v in window],
            'avg': round(sum(window) / len(window), 2),
            'max': round(max(window), 2),
            'min': round(min(window), 2)
        })
