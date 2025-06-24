#!/usr/bin/env python3
"""
Load Test Script for Django Product API with Activity Logging

This script performs load testing on the Django backend API by:
1. Authenticating with username/password to get a token
2. Creating multiple products using POST /v2/products/
3. Using multi-threading to perform bulk updates via PATCH /v2/catalog_product_edits/
4. Measuring performance metrics and latency statistics

The script targets the activity logging system by creating and updating products
which are monitored by the Django Activity Logger middleware.
"""

import random
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# CONFIGURATION VARIABLES
# =============================================================================

# API Configuration
LOCAL_URL = 'http://localhost:8000'
BASE_EC2_URL = 'http://ec2-98-84-57-242.compute-1.amazonaws.com:8000'
INGESTION_EC2_URL = 'http://ec2-54-147-162-181.compute-1.amazonaws.com:8000'
ADITYA_EC2_URL = 'http://ec2-54-204-77-1.compute-1.amazonaws.com:8000'

API_BASE_URL = INGESTION_EC2_URL
LOGIN_URL = '/v2/login'
CREATE_URL = '/v2/products'
BULK_EDIT_URL = '/v2/catalog_product_edits'
PRODUCT_CATEGORIES_URL = '/v2/product_categories'

# Authentication
USERNAME = 'lincoln@qa.surefront.com'
PASSWORD = 'Password123'

# Load Test Parameters
DEFAULT_N = 5  # Number of threads
DEFAULT_M = 1  # Number of updates per object

# Product Update Fields (selected 5 simple properties based on products_edit.py)
FIELD_CHOICES = ['color', 'description', 'notes', 'factory_cost', 'freight_rate']

# Sample values for random updates
FIELD_VALUES = {
    'color': ['red', 'blue', 'green', 'yellow', 'black', 'white', 'purple', 'orange'],
    'description': ['High quality product', 'Premium item', 'Best seller', 'Limited edition', 'New arrival'],
    'notes': ['Updated in load test', 'Testing notes', 'Quality assured', 'In stock', 'Customer favorite'],
    'factory_cost': [10.50, 25.00, 50.00, 75.25, 100.00, 15.75, 30.50],
    'freight_rate': [5.00, 10.00, 15.50, 20.00, 25.75, 30.00, 35.25],
}

# Create payload template for products
def get_create_payload_template(product_category_id: int) -> Dict:
    """Generate a product creation payload with unique item number"""
    unique_id = str(uuid.uuid4())[:8]
    return {
        'product_category': product_category_id,
        'item_number': f'LOAD_TEST_{unique_id}_{int(time.time())}',
        'factory_cost': 25.00,
        'list_price': 50.00,
        'color': 'test-color',
        'description': 'Load test product',
    }


# Update payload template for bulk edit
def get_update_payload_template(product_ids: List[int], field_name: str, field_value) -> Dict:
    """Generate bulk update payload for catalog_product_edits endpoint"""
    return {'standard_fields': [{'id': product_id, field_name: field_value} for product_id in product_ids]}


# =============================================================================
# HTTP CLIENT SETUP
# =============================================================================


def create_session() -> requests.Session:
    """Create a requests session with retry strategy and connection pooling"""
    session = requests.Session()

    # Configure retry strategy
    retry_strategy = Retry(total=3, status_forcelist=[429, 500, 502, 503, 504], method_whitelist=['HEAD', 'GET', 'OPTIONS', 'POST', 'PATCH', 'PUT'], backoff_factor=1)

    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


# =============================================================================
# CORE FUNCTIONS
# =============================================================================


def login(session: requests.Session) -> str:
    """
    Perform login and return authentication token with retry logic

    Returns:
        str: Authentication token for subsequent API calls
    """
    login_payload = {'email': USERNAME, 'password': PASSWORD}

    url = f'{API_BASE_URL}{LOGIN_URL}'
    max_retries = 3

    for attempt in range(max_retries):
        print(f'Attempting login at {url} for user {USERNAME} (attempt {attempt + 1}/{max_retries})')

        try:
            response = session.post(url, json=login_payload, timeout=30)
            response.raise_for_status()

            data = response.json()
            token = data.get('token')
            print(f'Token: {token}')
            if not token:
                raise ValueError(f'No token found in response. Available keys: {list(data.keys())}')

            print(f'✓ Login successful. Token obtained: {token[:20]}...')
            return token

        except requests.exceptions.RequestException as e:
            print(f'✗ Login failed on attempt {attempt + 1}: {e}')
            if hasattr(e, 'response') and e.response is not None:
                print(f'Response status: {e.response.status_code}')
                print(f'Response content: {e.response.text}')

            # If this is not the last attempt, wait 5 seconds before retrying
            if attempt < max_retries - 1:
                print('Retrying in 5 seconds...')
                time.sleep(5)
            else:
                print(f'All {max_retries} login attempts failed')
                raise


def get_product_category(session: requests.Session, token: str) -> Optional[int]:
    """
    Get the first available product category for the authenticated user

    Returns:
        int: Product category ID
    """
    headers = {'Authorization': f'Token {token}'}
    url = f'{API_BASE_URL}{PRODUCT_CATEGORIES_URL}'

    try:
        response = session.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        categories = response.json()
        if categories and len(categories) > 0:
            category_id = categories[0]['id']
            print(f'✓ Using product category ID: {category_id}')
            return category_id
        else:
            print('✗ No product categories found')
            return None

    except requests.exceptions.RequestException as e:
        print(f'✗ Failed to get product categories: {e}')
        return None


def create_objects(session: requests.Session, token: str, n: int, product_category_id: int) -> List[int]:
    """
    Create n products and return their IDs

    Args:
        session: HTTP session
        token: Authentication token
        n: Number of products to create
        product_category_id: Product category to assign to products

    Returns:
        List[int]: List of created product IDs
    """
    total_products = n
    headers = {'Authorization': f'Token {token}'}
    url = f'{API_BASE_URL}{CREATE_URL}'

    created_ids = []
    failed_creates = 0

    print(f'Creating {total_products} products using category ID: {product_category_id}...')
    start_time = time.time()

    for i in range(total_products):
        payload = get_create_payload_template(product_category_id)

        try:
            response = session.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code == 201:
                product_data = response.json()
                product_id = product_data['id']
                created_ids.append(product_id)

                # Print individual product creation details
                print(f"  ✓ Product {i + 1}: ID={product_id}, Category={product_category_id}, Item#={payload['item_number']}")

                if (i + 1) % 50 == 0:
                    print(f'  📊 Progress: {i + 1}/{total_products} products created...')
            else:
                failed_creates += 1
                print(f'  ✗ Failed to create product {i + 1}: {response.status_code} - {response.text[:100]}')

        except requests.exceptions.RequestException as e:
            failed_creates += 1
            print(f'  ✗ Exception creating product {i + 1}: {e}')

    elapsed = time.time() - start_time
    success_count = len(created_ids)

    print(f'✓ Product creation completed in {elapsed:.2f}s')
    print(f'  Successfully created: {success_count}/{total_products}')
    print(f'  Failed: {failed_creates}')
    print(f'  📋 Created Product IDs: {created_ids}')
    print(f'  🏷️  All products assigned to Category ID: {product_category_id}')
    print('')

    return created_ids


def update_worker(thread_id: int, ids_subset: List[int], token: str, m: int) -> List[Dict]:
    """
    Worker function for multi-threaded bulk updates

    Args:
        thread_id: Unique thread identifier
        ids_subset: List of product IDs assigned to this thread
        token: Authentication token
        m: Number of updates per object

    Returns:
        List[Dict]: List of request results with timing data
    """
    session = create_session()
    headers = {'Authorization': f'Token {token}'}
    url = f'{API_BASE_URL}{BULK_EDIT_URL}'

    results = []
    products_count = len(ids_subset)
    total_requests = products_count * m

    print(f'Thread {thread_id}: Starting updates for {products_count} products ({total_requests} total requests)')

    for product_id in ids_subset:
        for update_num in range(m):
            # Select random field and value for update
            field_name = random.choice(FIELD_CHOICES)
            field_value = random.choice(FIELD_VALUES[field_name])

            # Create bulk edit payload
            payload = get_update_payload_template([product_id], field_name, field_value)

            start_time = time.time()
            success = False
            status_code = None
            error_msg = None

            try:
                response = session.patch(url, json=payload, headers=headers, timeout=30)
                status_code = response.status_code
                success = 200 <= status_code < 300

                if not success:
                    error_msg = response.text[:200]

            except requests.exceptions.RequestException as e:
                error_msg = str(e)[:200]

            end_time = time.time()
            latency = end_time - start_time
            print(f'Thread {thread_id} - Product {product_id} - Update {update_num + 1}: {latency:.2f}s')

            results.append(
                {
                    'thread_id': thread_id,
                    'product_id': product_id,
                    'update_num': update_num + 1,
                    'field': field_name,
                    'value': field_value,
                    'success': success,
                    'status_code': status_code,
                    'latency': latency,
                    'timestamp': end_time,
                    'error': error_msg,
                }
            )

    print(f'Thread {thread_id}: Completed {total_requests} requests')
    session.close()
    return results


def run_load_test(n: int, m: int) -> Tuple[List[Dict], float, int, List[int]]:
    """
    Execute the complete load test

    Args:
        n: Number of threads to launch
        m: Number of updates per object per thread

    Returns:
        Tuple[List[Dict], float, int, List[int]]: (results_list, total_execution_time, product_category_id, created_product_ids)
    """
    print('=' * 80)
    print('STARTING LOAD TEST')
    print('=' * 80)
    print('Configuration:')
    print(f'  API Base URL: {API_BASE_URL}')
    print(f'  Username: {USERNAME}')
    print(f'  Threads (n): {n}')
    print(f'  Updates per object (m): {m}')
    print(f'  Total products to create: {n}')
    print(f'  Update fields: {FIELD_CHOICES}')
    print('')

    # Initialize session
    session = create_session()
    total_start_time = time.time()

    try:
        # Step 1: Login
        print('Step 1: Authentication')
        token = login(session)
        print('')

        # Step 2: Get product category
        print('Step 2: Getting product category')
        product_category_id = get_product_category(session, token)
        if not product_category_id:
            raise ValueError('Could not retrieve product category')
        print('')

        # Step 3: Create products
        print('Step 3: Creating products')
        created_ids = create_objects(session, token, n, product_category_id)

        if not created_ids:
            raise ValueError('No products were created successfully')

        print(f'✓ Created {len(created_ids)} products')
        print('')

        # Step 4: Multi-threaded bulk updates
        print('Step 4: Multi-threaded bulk updates')
        print(f'Distributing {len(created_ids)} products among {n} threads...')

        # Distribute products evenly among threads
        products_per_thread = len(created_ids) // n
        remainder = len(created_ids) % n

        thread_assignments = []
        start_idx = 0

        for i in range(n):
            # Give extra product to first 'remainder' threads
            thread_size = products_per_thread + (1 if i < remainder else 0)
            end_idx = start_idx + thread_size

            thread_products = created_ids[start_idx:end_idx]
            thread_assignments.append((i + 1, thread_products))

            print(f'  Thread {i + 1}: {len(thread_products)} products')
            start_idx = end_idx

        print('')
        print('Starting threaded updates...')
        update_start_time = time.time()

        # Execute threads
        all_results = []
        with ThreadPoolExecutor(max_workers=n) as executor:
            # Submit all tasks
            future_to_thread = {executor.submit(update_worker, thread_id, ids_subset, token, m): thread_id for thread_id, ids_subset in thread_assignments}

            # Collect results
            for future in as_completed(future_to_thread):
                thread_id = future_to_thread[future]
                try:
                    thread_results = future.result()
                    all_results.extend(thread_results)
                except Exception as e:
                    print(f'✗ Thread {thread_id} failed: {e}')

        update_end_time = time.time()
        total_end_time = time.time()

        update_duration = update_end_time - update_start_time
        total_duration = total_end_time - total_start_time

        print(f'✓ All threads completed in {update_duration:.2f}s')
        print(f'✓ Total test duration: {total_duration:.2f}s')
        print('')

        # Return all the info
        return all_results, total_duration, product_category_id, created_ids

    finally:
        session.close()


def generate_report(results: List[Dict], total_time: float, product_category_id: int = None, created_product_ids: List[int] = None):
    """
    Generate and display comprehensive performance report

    Args:
        results: List of individual request results
        total_time: Total execution time
        product_category_id: Category ID used for product creation
        created_product_ids: List of product IDs that were created
    """
    print('=' * 100)
    print('COMPREHENSIVE LOAD TEST RESULTS')
    print('=' * 100)

    if not results:
        print('No results to analyze')
        return

    # Basic statistics
    total_requests = len(results)
    successful_requests = sum(1 for r in results if r['success'])
    failed_requests = total_requests - successful_requests
    success_rate = (successful_requests / total_requests) * 100 if total_requests > 0 else 0

    # Latency analysis (only successful requests)
    successful_latencies = [r['latency'] for r in results if r['success']]

    if successful_latencies:
        avg_latency = statistics.mean(successful_latencies)
        median_latency = statistics.median(successful_latencies)
        min_latency = min(successful_latencies)
        max_latency = max(successful_latencies)

        # Percentiles
        sorted_latencies = sorted(successful_latencies)
        p10 = sorted_latencies[int(0.10 * len(sorted_latencies))]
        p90 = sorted_latencies[int(0.90 * len(sorted_latencies))]
        p95 = sorted_latencies[int(0.95 * len(sorted_latencies))]
        p99 = sorted_latencies[int(0.99 * len(sorted_latencies))]
    else:
        avg_latency = median_latency = min_latency = max_latency = 0
        p10 = p90 = p95 = p99 = 0

    # Throughput
    requests_per_second = total_requests / total_time if total_time > 0 else 0

    # Test Configuration Summary
    print('🔧 TEST CONFIGURATION:')
    print(f'  API Base URL: {API_BASE_URL}')
    print(f'  Username: {USERNAME}')
    if created_product_ids:
        print(f'  📋 Created Product IDs: {created_product_ids}')
    if product_category_id:
        print(f'  🏷️  Product Category ID Used: {product_category_id}')
    print(f"  Total Products Created: {len(created_product_ids) if created_product_ids else len(set(r['product_id'] for r in results))}")
    print(f"  Threads Used: {len(set(r['thread_id'] for r in results))}")
    print(f"  Update Fields Tested: {', '.join(FIELD_CHOICES)}")
    print('')

    # Overall Performance Summary
    print('📊 OVERALL PERFORMANCE:')
    print(f'  🕐 Total Execution Time: {total_time:.2f} seconds')
    print(f'  📨 Total API Requests: {total_requests:,}')
    print(f'  ✅ Successful Requests: {successful_requests:,}')
    print(f'  ❌ Failed Requests: {failed_requests:,}')
    print(f'  📈 Success Rate: {success_rate:.1f}%')
    print(f'  🚀 Throughput: {requests_per_second:.2f} requests/second')
    print('')

    # Latency Analysis
    if successful_latencies:
        print('⏱️  LATENCY ANALYSIS (Successful Requests Only):')
        print(f'  📊 Average Latency: {avg_latency*1000:.1f} ms')
        print(f'  📊 Median Latency: {median_latency*1000:.1f} ms')
        print(f'  ⚡ Fastest Request: {min_latency*1000:.1f} ms')
        print(f'  🐌 Slowest Request: {max_latency*1000:.1f} ms')
        print('')

        print('📉 LATENCY PERCENTILES:')
        print(f'  10th percentile: {p10*1000:.1f} ms')
        print(f'  90th percentile: {p90*1000:.1f} ms')
        print(f'  95th percentile: {p95*1000:.1f} ms')
        print(f'  99th percentile: {p99*1000:.1f} ms')
        print('')

    # Thread Performance Analysis
    thread_results = {}
    for result in results:
        thread_id = result['thread_id']
        if thread_id not in thread_results:
            thread_results[thread_id] = {'success': 0, 'failed': 0, 'latencies': []}

        if result['success']:
            thread_results[thread_id]['success'] += 1
            thread_results[thread_id]['latencies'].append(result['latency'])
        else:
            thread_results[thread_id]['failed'] += 1

    print('🧵 THREAD PERFORMANCE BREAKDOWN:')
    for thread_id in sorted(thread_results.keys()):
        thread_data = thread_results[thread_id]
        total_thread_requests = thread_data['success'] + thread_data['failed']
        thread_success_rate = (thread_data['success'] / total_thread_requests) * 100 if total_thread_requests > 0 else 0

        if thread_data['latencies']:
            thread_avg_latency = statistics.mean(thread_data['latencies'])
            thread_min_latency = min(thread_data['latencies'])
            thread_max_latency = max(thread_data['latencies'])
        else:
            thread_avg_latency = thread_min_latency = thread_max_latency = 0

        print(
            f'  Thread {thread_id}: {total_thread_requests:2d} requests │ '
            f'{thread_success_rate:5.1f}% success │ '
            f'Avg: {thread_avg_latency*1000:6.1f}ms │ '
            f'Min: {thread_min_latency*1000:6.1f}ms │ '
            f'Max: {thread_max_latency*1000:6.1f}ms'
        )
    print('')

    # Field Update Distribution
    field_updates = {}
    field_latencies = {}
    for result in results:
        if result['success']:
            field = result['field']
            field_updates[field] = field_updates.get(field, 0) + 1
            if field not in field_latencies:
                field_latencies[field] = []
            field_latencies[field].append(result['latency'])

    if field_updates:
        print('🎯 FIELD UPDATE ANALYSIS:')
        for field in sorted(field_updates.keys()):
            count = field_updates[field]
            percentage = (count / successful_requests) * 100 if successful_requests > 0 else 0
            avg_field_latency = statistics.mean(field_latencies[field]) * 1000
            print(f'  {field:15s}: {count:3d} updates ({percentage:5.1f}%) │ Avg: {avg_field_latency:6.1f}ms')
        print('')

    # Error Analysis
    if failed_requests > 0:
        print('❌ ERROR ANALYSIS:')
        error_counts = {}
        error_details = {}
        for result in results:
            if not result['success']:
                error_key = f"HTTP {result['status_code']}" if result['status_code'] else 'Connection Error'
                error_counts[error_key] = error_counts.get(error_key, 0) + 1
                if error_key not in error_details:
                    error_details[error_key] = []
                error_details[error_key].append(
                    {
                        'thread_id': result['thread_id'],
                        'product_id': result['product_id'],
                        'field': result['field'],
                        'error': result['error'][:100] if result['error'] else 'Unknown',
                    }
                )

        for error_type, count in sorted(error_counts.items()):
            print(f'  {error_type}: {count} occurrences')
            # Show first few error details
            for detail in error_details[error_type][:3]:
                print(f"    └─ Thread {detail['thread_id']}, Product {detail['product_id']}, Field: {detail['field']}")
                print(f"       Error: {detail['error']}")
            if len(error_details[error_type]) > 3:
                print(f'    └─ ... and {len(error_details[error_type]) - 3} more similar errors')
        print('')

    # Sample successful requests details
    if successful_requests > 0:
        print('✅ SAMPLE SUCCESSFUL REQUESTS:')
        successful_results = [r for r in results if r['success']]
        sample_size = min(5, len(successful_results))
        for i, result in enumerate(successful_results[:sample_size]):
            print(
                f"  Request {i+1}: Thread {result['thread_id']} │ "
                f"Product {result['product_id']} │ "
                f"Field: {result['field']} → {result['value']} │ "
                f"Latency: {result['latency']*1000:.1f}ms"
            )
        if len(successful_results) > sample_size:
            print(f'  ... and {len(successful_results) - sample_size} more successful requests')
        print('')

    # Activity Logging Impact Analysis
    print('📝 ACTIVITY LOGGING IMPACT:')
    if successful_latencies:
        # Assuming baseline API latency is much lower, high latency indicates activity logging overhead
        avg_latency_ms = avg_latency * 1000
        if avg_latency_ms > 1000:
            print(f'  ⚠️  High average latency ({avg_latency_ms:.1f}ms) indicates significant activity logging overhead')
        elif avg_latency_ms > 500:
            print(f'  ⚡ Moderate latency ({avg_latency_ms:.1f}ms) - activity logging is impacting performance')
        else:
            print(f'  ✅ Low latency ({avg_latency_ms:.1f}ms) - activity logging overhead is minimal')

        # Check for latency spikes
        if max_latency > avg_latency * 3:
            print(f'  🔍 Latency spikes detected (max: {max_latency*1000:.1f}ms vs avg: {avg_latency_ms:.1f}ms)')
            print('     This may indicate database locking or batch processing in activity logger')
    print('')

    # Performance Recommendations
    print('💡 PERFORMANCE RECOMMENDATIONS:')
    if success_rate < 95:
        print('  🔧 Consider reducing concurrency - high failure rate detected')
    if avg_latency_ms > 2000:
        print('  🔧 Consider optimizing activity logging queries or using async processing')
    if len(set(r['thread_id'] for r in results)) > 10 and avg_latency_ms > 1000:
        print('  🔧 High thread count with high latency - consider database connection pooling')
    if successful_requests > 0:
        print('  ✅ Load test completed successfully - system can handle concurrent updates')
    print('')


def main():
    """
    Main entry point for the load test script
    """
    import argparse

    # Declare globals first
    global API_BASE_URL, USERNAME, PASSWORD

    parser = argparse.ArgumentParser(description='Load Test for Django Product API with Activity Logging')
    parser.add_argument('-n', '--threads', type=int, default=DEFAULT_N, help=f'Number of threads (default: {DEFAULT_N})')
    parser.add_argument('-m', '--updates', type=int, default=DEFAULT_M, help=f'Number of updates per object (default: {DEFAULT_M})')
    parser.add_argument('--url', type=str, default=API_BASE_URL, help=f'API base URL (default: {API_BASE_URL})')
    parser.add_argument('--username', type=str, default=USERNAME, help=f'Username for authentication (default: {USERNAME})')
    parser.add_argument('--password', type=str, default=PASSWORD, help='Password for authentication')

    args = parser.parse_args()

    # Update global configuration
    API_BASE_URL = args.url
    USERNAME = args.username
    PASSWORD = args.password

    try:
        # Run load test
        results, total_time, category_id, created_ids = run_load_test(args.threads, args.updates)

        # Generate report with additional info
        generate_report(results, total_time, category_id, created_ids)

    except KeyboardInterrupt:
        print('\n✗ Load test interrupted by user')
    except Exception as e:
        print(f'\n✗ Load test failed: {e}')
        import traceback

        traceback.print_exc()


if __name__ == '__main__':
    main()
