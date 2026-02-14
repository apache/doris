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
Scalar Python UDF operations - row-by-row processing
"""

import math
import re
from datetime import datetime, timedelta
from decimal import Decimal


# ==================== Numeric Operations ====================

def add_three_numbers(a, b, c):
    """Add three numbers"""
    if a is None or b is None or c is None:
        return None
    return a + b + c


def multiply_with_default(a, b, default=1):
    """Multiply two numbers, return default if any is None"""
    if a is None or b is None:
        return default
    return a * b


def safe_divide_with_precision(numerator, denominator, precision=2):
    """Safe division with specified decimal precision"""
    if numerator is None or denominator is None or denominator == 0:
        return None
    result = numerator / denominator
    return round(result, precision)


def calculate_discount_price(original_price, discount_percent):
    """Calculate price after discount"""
    if original_price is None or discount_percent is None:
        return None
    if discount_percent < 0 or discount_percent > 100:
        return original_price
    return original_price * (1 - discount_percent / 100)


def compound_interest(principal, rate, years):
    """Calculate compound interest: P * (1 + r)^t"""
    if principal is None or rate is None or years is None:
        return None
    if principal <= 0 or rate < 0 or years < 0:
        return None
    return principal * math.pow(1 + rate / 100, years)


def calculate_bmi(weight_kg, height_m):
    """Calculate Body Mass Index"""
    if weight_kg is None or height_m is None or height_m <= 0:
        return None
    return round(weight_kg / (height_m * height_m), 2)


def fibonacci(n):
    """Calculate nth Fibonacci number"""
    if n is None or n < 0:
        return None
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b


def is_prime(n):
    """Check if a number is prime"""
    if n is None or n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True


def gcd(a, b):
    """Calculate Greatest Common Divisor"""
    if a is None or b is None:
        return None
    a, b = abs(a), abs(b)
    while b:
        a, b = b, a % b
    return a


def lcm(a, b):
    """Calculate Least Common Multiple"""
    if a is None or b is None or a == 0 or b == 0:
        return None
    return abs(a * b) // gcd(a, b)


# ==================== String Operations ====================

def reverse_string(s):
    """Reverse a string"""
    if s is None:
        return None
    return s[::-1]


def count_vowels(s):
    """Count number of vowels in a string"""
    if s is None:
        return None
    vowels = 'aeiouAEIOU'
    return sum(1 for char in s if char in vowels)


def count_words(s):
    """Count number of words in a string"""
    if s is None:
        return None
    return len(s.split())


def string_length_custom(s):
    """Calculate string length (custom implementation for testing)"""
    if s is None:
        return None
    return len(s)


def capitalize_words(s):
    """Capitalize first letter of each word"""
    if s is None:
        return None
    return ' '.join(word.capitalize() for word in s.split())


def remove_whitespace(s):
    """Remove all whitespace from string"""
    if s is None:
        return None
    return ''.join(s.split())


def extract_numbers(s):
    """Extract all numbers from string and concatenate"""
    if s is None:
        return None
    numbers = re.findall(r'\d+', s)
    return ','.join(numbers) if numbers else ''


def is_palindrome(s):
    """Check if string is a palindrome (case-insensitive)"""
    if s is None:
        return None
    cleaned = ''.join(c.lower() for c in s if c.isalnum())
    return cleaned == cleaned[::-1]


def string_similarity(s1, s2):
    """Calculate simple string similarity (0-100)"""
    if s1 is None or s2 is None:
        return None
    if s1 == s2:
        return 100.0
    # Simple character overlap ratio
    set1, set2 = set(s1.lower()), set(s2.lower())
    if not set1 or not set2:
        return 0.0
    intersection = len(set1 & set2)
    union = len(set1 | set2)
    return round(intersection / union * 100, 2)


def mask_email(email):
    """Mask email address: user@domain.com -> u***@domain.com"""
    if email is None or '@' not in email:
        return None
    parts = email.split('@')
    if len(parts[0]) <= 1:
        return email
    masked_user = parts[0][0] + '***'
    return f"{masked_user}@{parts[1]}"


def extract_domain(email):
    """Extract domain from email address"""
    if email is None or '@' not in email:
        return None
    return email.split('@')[1]


def truncate_string(s, max_length, suffix='...'):
    """Truncate string to max length with suffix"""
    if s is None:
        return None
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


# ==================== Date/Time Operations ====================

def days_between_dates(date1_str, date2_str):
    """Calculate days between two dates (YYYY-MM-DD format)"""
    if date1_str is None or date2_str is None:
        return None
    try:
        d1 = datetime.strptime(str(date1_str), '%Y-%m-%d')
        d2 = datetime.strptime(str(date2_str), '%Y-%m-%d')
        return abs((d2 - d1).days)
    except:
        return None


def is_weekend(date_str):
    """Check if date is weekend (Saturday or Sunday)"""
    if date_str is None:
        return None
    try:
        date = datetime.strptime(str(date_str), '%Y-%m-%d')
        return date.weekday() >= 5  # 5=Saturday, 6=Sunday
    except:
        return None


def get_quarter(date_str):
    """Get quarter (1-4) from date"""
    if date_str is None:
        return None
    try:
        date = datetime.strptime(str(date_str), '%Y-%m-%d')
        return (date.month - 1) // 3 + 1
    except:
        return None


def age_in_years(birth_date_str, current_date_str):
    """Calculate age in years"""
    if birth_date_str is None or current_date_str is None:
        return None
    try:
        birth = datetime.strptime(str(birth_date_str), '%Y-%m-%d')
        current = datetime.strptime(str(current_date_str), '%Y-%m-%d')
        age = current.year - birth.year
        if (current.month, current.day) < (birth.month, birth.day):
            age -= 1
        return age
    except:
        return None


# ==================== Boolean/Conditional Operations ====================

def is_in_range(value, min_val, max_val):
    """Check if value is in range [min_val, max_val]"""
    if value is None or min_val is None or max_val is None:
        return None
    return min_val <= value <= max_val


def xor_operation(a, b):
    """XOR operation on two booleans"""
    if a is None or b is None:
        return None
    return (a or b) and not (a and b)


def all_true(*args):
    """Check if all arguments are True"""
    if any(arg is None for arg in args):
        return None
    return all(args)


def any_true(*args):
    """Check if any argument is True"""
    if any(arg is None for arg in args):
        return None
    return any(args)


def count_true(*args):
    """Count number of True values"""
    if any(arg is None for arg in args):
        return None
    return sum(1 for arg in args if arg)


# ==================== Complex/Mixed Operations ====================

def calculate_grade(score):
    """Convert numeric score to letter grade"""
    if score is None:
        return None
    if score >= 90:
        return 'A'
    elif score >= 80:
        return 'B'
    elif score >= 70:
        return 'C'
    elif score >= 60:
        return 'D'
    else:
        return 'F'


def categorize_age(age):
    """Categorize age into groups"""
    if age is None:
        return None
    if age < 0:
        return 'Invalid'
    elif age < 13:
        return 'Child'
    elif age < 20:
        return 'Teenager'
    elif age < 60:
        return 'Adult'
    else:
        return 'Senior'


def calculate_tax(income, tax_rate):
    """Calculate tax with progressive rates"""
    if income is None or tax_rate is None:
        return None
    if income <= 0:
        return 0.0
    return round(income * tax_rate / 100, 2)


def format_phone_number(phone):
    """Format phone number: 1234567890 -> (123) 456-7890"""
    if phone is None:
        return None
    digits = ''.join(c for c in str(phone) if c.isdigit())
    if len(digits) != 10:
        return phone
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


def validate_credit_card_luhn(card_number):
    """Validate credit card using Luhn algorithm"""
    if card_number is None:
        return False
    digits = [int(d) for d in str(card_number) if d.isdigit()]
    if not digits:
        return False
    
    checksum = 0
    for i, digit in enumerate(reversed(digits)):
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        checksum += digit
    return checksum % 10 == 0


def json_extract_value(json_str, key):
    """Extract value from simple JSON string"""
    if json_str is None or key is None:
        return None
    try:
        import json
        data = json.loads(json_str)
        return str(data.get(key, ''))
    except:
        return None


def levenshtein_distance(s1, s2):
    """Calculate Levenshtein distance between two strings"""
    if s1 is None or s2 is None:
        return None
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)
    
    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    
    return previous_row[-1]
