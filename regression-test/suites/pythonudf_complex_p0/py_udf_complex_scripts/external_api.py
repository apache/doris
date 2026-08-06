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
External API Integration

All APIs used in this module are free and do not require API keys:
- JSONPlaceholder (jsonplaceholder.typicode.com) - Fake REST API, FIXED responses
- ip-api.com - IP geolocation
- Open-Meteo (open-meteo.com) - Weather API
- WorldTimeAPI (worldtimeapi.org) - Timezone API
"""

import json
import urllib.request
import urllib.error
import ssl


def _http_get(url, timeout=30):
    """
    Internal helper: Make HTTP GET request using urllib (built-in).
    Returns parsed JSON or raises exception.
    """
    # Create SSL context that doesn't verify certificates (for compatibility)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    req = urllib.request.Request(url, headers={'User-Agent': 'Doris-Python-UDF/1.0'})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as response:
        return json.loads(response.read().decode('utf-8'))


def _http_post(url, data, timeout=30):
    """
    Internal helper: Make HTTP POST request using urllib (built-in).
    Returns parsed JSON or raises exception.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    json_data = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(
        url,
        data=json_data,
        headers={
            'User-Agent': 'Doris-Python-UDF/1.0',
            'Content-Type': 'application/json'
        }
    )
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as response:
        return json.loads(response.read().decode('utf-8'))


# ==================== Fixed Response APIs (JSONPlaceholder) ====================
# JSONPlaceholder is a fake REST API with FIXED data, suitable for deterministic tests.

def fetch_post_title(post_id):
    """
    Fetch a post title from JSONPlaceholder (FIXED response).
    e.g., post_id=1 always returns:
      "sunt aut facere repellat provident occaecati excepturi optio reprehenderit"
    """
    if post_id is None:
        return None

    try:
        data = _http_get(f'https://jsonplaceholder.typicode.com/posts/{int(post_id)}')
        return data.get('title')
    except Exception:
        return None


def fetch_user_email(user_id):
    """
    Fetch a user email from JSONPlaceholder (FIXED response).
    e.g., user_id=1 always returns "Sincere@april.biz"
    """
    if user_id is None:
        return None

    try:
        data = _http_get(f'https://jsonplaceholder.typicode.com/users/{int(user_id)}')
        return data.get('email')
    except Exception:
        return None


def fetch_user_name(user_id):
    """
    Fetch a user name from JSONPlaceholder (FIXED response).
    e.g., user_id=1 always returns "Leanne Graham"
    """
    if user_id is None:
        return None

    try:
        data = _http_get(f'https://jsonplaceholder.typicode.com/users/{int(user_id)}')
        return data.get('name')
    except Exception:
        return None


def fetch_todo_completed(todo_id):
    """
    Fetch a todo's completed status from JSONPlaceholder (FIXED response).
    e.g., todo_id=1 always returns "false"
    """
    if todo_id is None:
        return None

    try:
        data = _http_get(f'https://jsonplaceholder.typicode.com/todos/{int(todo_id)}')
        completed = data.get('completed')
        return str(completed).lower()
    except Exception:
        return None


def fetch_comments_count(post_id):
    """
    Fetch the number of comments for a post from JSONPlaceholder (FIXED response).
    e.g., post_id=1 always returns 5
    """
    if post_id is None:
        return -1

    try:
        data = _http_get(f'https://jsonplaceholder.typicode.com/posts/{int(post_id)}/comments')
        return len(data)
    except Exception:
        return -1


# ==================== HTTP Status Check ====================

def check_http_status(url):
    """
    Check the HTTP status code of a URL.
    Returns HTTP status code (INT), e.g., 200, 404, 500. Returns -1 on connection error.
    """
    if url is None:
        return -1

    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        req = urllib.request.Request(str(url), headers={'User-Agent': 'Doris-Python-UDF/1.0'})
        with urllib.request.urlopen(req, timeout=30, context=ctx) as response:
            return response.getcode()
    except urllib.error.HTTPError as e:
        return e.code
    except Exception:
        return -1


# ==================== Generic HTTP Functions ====================

def http_get_json(url, timeout_seconds=30):
    """
    Make HTTP GET request and return response as JSON string.
    """
    if url is None:
        return json.dumps({'error': 'url is required'})

    try:
        timeout = int(timeout_seconds) if timeout_seconds else 30
        data = _http_get(str(url), timeout=timeout)
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e)})


def http_post_json(url, body_json, timeout_seconds=30):
    """
    Make HTTP POST request with JSON body.
    """
    if url is None or body_json is None:
        return json.dumps({'error': 'url and body_json are required'})

    try:
        timeout = int(timeout_seconds) if timeout_seconds else 30
        body = json.loads(body_json)
        data = _http_post(str(url), body, timeout=timeout)
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== IP Geolocation (ip-api.com, free, no key) ====================

def get_ip_location(ip_address):
    """
    Get geolocation for an IP address via ip-api.com (free, no API key).
    DYNAMIC response - country/city may change with database updates.
    """
    if ip_address is None:
        return json.dumps({'error': 'ip_address is required'})

    try:
        data = _http_get(f'http://ip-api.com/json/{ip_address}', timeout=10)
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Weather (Open-Meteo, free, no key) ====================

def get_weather(latitude, longitude):
    """
    Get current weather via Open-Meteo API (free, no API key).
    DYNAMIC response - weather changes constantly.
    """
    if latitude is None or longitude is None:
        return json.dumps({'error': 'latitude and longitude are required'})

    try:
        url = (
            f'https://api.open-meteo.com/v1/forecast'
            f'?latitude={latitude}&longitude={longitude}'
            f'&current_weather=true'
        )
        data = _http_get(url, timeout=10)
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== World Time (WorldTimeAPI, free, no key) ====================

def get_world_time(timezone_str):
    """
    Get current time for a timezone via WorldTimeAPI (free, no API key).
    DYNAMIC response - time changes constantly.
    """
    if timezone_str is None:
        return json.dumps({'error': 'timezone_str is required'})

    try:
        data = _http_get(f'http://worldtimeapi.org/api/timezone/{timezone_str}', timeout=10)
        return json.dumps(data, ensure_ascii=False)
    except Exception as e:
        return json.dumps({'error': str(e)})
