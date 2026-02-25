from flask import Flask, jsonify, Response, request
import requests
import json
import time
import threading
import queue
import signal
import sys
from datetime import datetime


app = Flask(__name__)

# Global variables
previous_stock_data_api1 = None
previous_stock_data_api2 = None
clients = set()
client_queues = {}


def fetch_stock_data_api1():
    """Function to fetch data from first API source"""
    try:
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'Host': '88.99.61.159:4000',
            'If-None-Match': 'W/"3b80-5FY+gUMGy2CzPm8HBz2ejQ"',
            'Origin': 'http://88.99.61.159:5000',
            'Referer': 'http://88.99.61.159:5000/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
        }

        response = requests.get(
            'http://88.99.61.159:4000/getdata',
            headers=headers,
            timeout=5
        )

        if response.status_code == 200:
            return response.json()
        else:
            return None

    except Exception as error:
        print(f'Error fetching stock data from API1: {error}')
        return None


def fetch_stock_data_api2():
    """Function to fetch data from second API source"""
    try:
        headers = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36'
        }

        response = requests.get(
            'http://94.130.136.44:4000/getdata',
            headers=headers,
            timeout=5
        )

        if response.status_code == 200:
            return response.json()
        else:
            return None

    except Exception as error:
        print(f'Error fetching stock data from API2: {error}')
        return None


def combine_api_data(data1, data2):
    """Combine data from both APIs"""
    combined = {
        'timestamp': datetime.now().isoformat(),
        'sources': {
            'api1': '88.99.61.159:4000',
            'api2': '94.130.136.44:4000'
        },
        'data': []
    }

    # Extract arrays from both APIs
    array1 = data1.get('data', data1) if isinstance(data1, dict) else (data1 if data1 else [])
    array2 = data2.get('data', data2) if isinstance(data2, dict) else (data2 if data2 else [])

    # Convert to list if not already
    if not isinstance(array1, list):
        array1 = []
    if not isinstance(array2, list):
        array2 = []

    # Create a dict to merge by Symbol
    stocks_dict = {}

    # Add stocks from API1
    for stock in array1:
        if stock.get('Symbol'):
            stocks_dict[stock['Symbol']] = {
                **stock,
                'source': 'api1',
                'sources': ['api1']
            }

    # Add or merge stocks from API2
    for stock in array2:
        if stock.get('Symbol'):
            symbol = stock['Symbol']
            if symbol in stocks_dict:
                stocks_dict[symbol]['sources'].append('api2')
                stocks_dict[symbol]['source'] = 'both'
                stocks_dict[symbol]['api2_data'] = stock
            else:
                stocks_dict[symbol] = {
                    **stock,
                    'source': 'api2',
                    'sources': ['api2']
                }

    # Convert back to array
    combined['data'] = list(stocks_dict.values())
    combined['totalStocks'] = len(combined['data'])
    combined['api1Count'] = len([s for s in combined['data'] if 'api1' in s['sources']])
    combined['api2Count'] = len([s for s in combined['data'] if 'api2' in s['sources']])
    combined['bothCount'] = len([s for s in combined['data'] if len(s['sources']) > 1])

    return combined


def get_rate_changed_stocks(current_data, previous_data):
    """Function to compare stock data and find ONLY rate changes"""
    if not previous_data or not current_data:
        return None

    changed_stocks = {}

    current_array = current_data.get('data', [])
    previous_array = previous_data.get('data', [])

    current_stocks = {}
    previous_stocks = {}

    if isinstance(current_array, list):
        for stock in current_array:
            if stock.get('Symbol'):
                current_stocks[stock['Symbol']] = stock

    if isinstance(previous_array, list):
        for stock in previous_array:
            if stock.get('Symbol'):
                previous_stocks[stock['Symbol']] = stock

    for symbol, current_stock in current_stocks.items():
        previous_stock = previous_stocks.get(symbol)

        try:
            current_rate = float(current_stock.get("Last Traded Price", 0))
            previous_rate = float(previous_stock.get("Last Traded Price", 0)) if previous_stock else None
        except (ValueError, TypeError):
            continue

        if previous_stock and current_rate != previous_rate:
            changed_stocks[symbol] = {
                **current_stock,
                'previousRate': previous_rate,
                'changeType': 'increase' if current_rate > previous_rate else 'decrease',
                'changeAmount': current_rate - previous_rate,
                'changePercent': ((current_rate - previous_rate) / previous_rate * 100) if previous_rate else 0
            }

    return changed_stocks if changed_stocks else None


def broadcast_to_clients(data, event_type='rateUpdate'):
    """Function to send data to all SSE clients"""
    if not client_queues:
        return

    message = {
        'event': event_type,
        'data': data
    }

    for client_id, client_queue in list(client_queues.items()):
        try:
            client_queue.put(message, block=False)
        except queue.Full:
            client_queues.pop(client_id, None)
            print(f'Removed client {client_id} due to full queue')


def data_polling_worker():
    """Background worker for data polling from both APIs"""
    global previous_stock_data_api1, previous_stock_data_api2

    while True:
        try:
            current_data_api1 = fetch_stock_data_api1()
            current_data_api2 = fetch_stock_data_api2()

            if current_data_api1 or current_data_api2:
                combined_current = combine_api_data(current_data_api1, current_data_api2)

                if previous_stock_data_api1 or previous_stock_data_api2:
                    combined_previous = combine_api_data(previous_stock_data_api1, previous_stock_data_api2)
                    rate_changed_stocks = get_rate_changed_stocks(combined_current, combined_previous)

                    if rate_changed_stocks:
                        print(f'Broadcasting rate changes for {len(rate_changed_stocks)} stocks')
                        broadcast_to_clients(rate_changed_stocks, 'rateUpdate')

                previous_stock_data_api1 = current_data_api1
                previous_stock_data_api2 = current_data_api2

        except Exception as error:
            print(f'Data fetch error: {error}')
            broadcast_to_clients({'error': 'Failed to fetch stock data'}, 'error')

        time.sleep(1)


@app.route('/events')
def events():
    """SSE endpoint - streams only rate changes from both APIs"""
    def event_stream():
        client_id = id(request)
        client_queue = queue.Queue(maxsize=100)
        client_queues[client_id] = client_queue

        print(f'Client {client_id} connected. Total clients: {len(client_queues)}')

        yield f"event: connected\ndata: {json.dumps({'message': 'Connected - monitoring both APIs for rate updates'})}\n\n"

        try:
            while True:
                try:
                    message = client_queue.get(timeout=30)
                    yield f"event: {message['event']}\ndata: {json.dumps(message['data'])}\n\n"
                except queue.Empty:
                    yield f"event: ping\ndata: {json.dumps({'timestamp': int(time.time() * 1000)})}\n\n"

        except GeneratorExit:
            client_queues.pop(client_id, None)
            print(f'Client {client_id} disconnected. Total clients: {len(client_queues)}')

    return Response(
        event_stream(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Cache-Control'
        }
    )


@app.route('/rate')
def rate():
    """Endpoint that returns combined data from both APIs"""
    try:
        data1 = fetch_stock_data_api1()
        data2 = fetch_stock_data_api2()

        if not data1 and not data2:
            raise Exception('Failed to fetch data from both APIs')

        combined = combine_api_data(data1, data2)
        return jsonify(combined)

    except Exception as error:
        print(f'Error fetching rates: {error}')
        return jsonify({
            'error': 'Failed to fetch rates',
            'details': str(error)
        }), 500


@app.route('/rate/api1')
def rate_api1():
    """Endpoint for API1 data only"""
    try:
        data = fetch_stock_data_api1()
        if not data:
            raise Exception('Failed to fetch data from API1')
        return jsonify(data)
    except Exception as error:
        return jsonify({'error': str(error)}), 500


@app.route('/rate/api2')
def rate_api2():
    """Endpoint for API2 data only"""
    try:
        data = fetch_stock_data_api2()
        if not data:
            raise Exception('Failed to fetch data from API2')
        return jsonify(data)
    except Exception as error:
        return jsonify({'error': str(error)}), 500


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'clients': len(client_queues),
        'hasApi1Data': previous_stock_data_api1 is not None,
        'hasApi2Data': previous_stock_data_api2 is not None,
        'timestamp': datetime.now().isoformat()
    })


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    print('Shutting down gracefully...')

    for client_queue in client_queues.values():
        try:
            client_queue.put({'event': 'shutdown', 'data': {'message': 'Server shutting down'}})
        except:
            pass

    client_queues.clear()
    sys.exit(0)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start background data polling worker only
    data_thread = threading.Thread(target=data_polling_worker, daemon=True)
    data_thread.start()

    print('Starting Flask app...')
    print('Monitoring APIs:')
    print('  - API1: http://88.99.61.159:4000/getdata')
    print('  - API2: http://94.130.136.44:4000/getdata')

    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
