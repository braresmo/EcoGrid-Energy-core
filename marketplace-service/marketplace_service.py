"""
Marketplace Service
Handles energy trading and marketplace operations
"""

from flask import Flask, jsonify, request
from shared.event_bus import EventBus
import logging
from datetime import datetime
from enum import Enum
import json
import threading

logger = logging.getLogger(__name__)
app = Flask(__name__)

# Initialize event bus
event_bus = EventBus()

# In-memory store for trades
trades = {}
trade_history = []


def _upsert_trade_history(trade_id, updated_trade):
    for index, existing_trade in enumerate(trade_history):
        if existing_trade.get('trade_id') == trade_id:
            trade_history[index] = updated_trade
            break


def _update_trade_status(trade_id, status, **extra_fields):
    trade = trades.get(trade_id)
    if not trade:
        return None

    trade['status'] = status
    trade.update(extra_fields)
    trades[trade_id] = trade
    _upsert_trade_history(trade_id, trade)
    return trade

# Consumer callback: listen for meter data and create trades
def on_meter_data_ingested(ch, method, properties, body):
    try:
        msg = json.loads(body)
        data = msg.get('data', {})
        meter_id = data.get('meter_id')
        consumption = data.get('consumption', 0)

        # Create a simple trade: buyer is consumer of meter, seller is 'grid'
        trade_id = f"TRADE_{datetime.utcnow().timestamp()}"
        trade = {
            'trade_id': trade_id,
            'buyer_id': f'user_{meter_id}',
            'seller_id': 'grid',
            'amount': consumption,
            'price': 5.0,  # fixed price per kWh for demo
            'status': TradingStatus.PENDING.value,
            'timestamp': datetime.utcnow().isoformat()
        }

        trades[trade_id] = trade
        trade_history.append(trade)

        # Publish trade.created
        event_bus.publish(
            exchange='marketplace_exchange',
            event_type='trade.created',
            data=trade
        )

        # Match the trade, but keep it pending until the wallet confirms payment.
        trade['status'] = TradingStatus.PENDING.value
        trade['matched_at'] = datetime.utcnow().isoformat()
        trades[trade_id] = trade
        _upsert_trade_history(trade_id, trade)

        matched = {
            'trade_id': trade_id,
            'buyer_id': trade['buyer_id'],
            'seller_id': trade['seller_id'],
            'amount': trade['amount'],
            'total_price': trade['amount'] * trade['price']
        }
        event_bus.publish(
            exchange='marketplace_exchange',
            event_type='TradeMatched',
            data={**matched, 'status': trade['status'], 'matched_at': trade['matched_at']}
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error handling meter data ingest: {str(e)}")
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            pass


def on_payment_processed(ch, method, properties, body):
    try:
        msg = json.loads(body)
        data = msg.get('data', {})
        trade_id = data.get('trade_id')

        updated_trade = _update_trade_status(
            trade_id,
            TradingStatus.COMPLETED.value,
            payment_received_at=datetime.utcnow().isoformat()
        )

        if updated_trade:
            event_bus.publish(
                exchange='marketplace_exchange',
                event_type='trade.completed',
                data={
                    'trade_id': trade_id,
                    'status': updated_trade['status'],
                    'buyer_id': updated_trade.get('buyer_id'),
                    'seller_id': updated_trade.get('seller_id'),
                    'amount': updated_trade.get('amount'),
                    'price': updated_trade.get('price'),
                    'payment_received_at': updated_trade.get('payment_received_at')
                }
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error handling payment processed event: {str(e)}")
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            pass


def on_payment_failed(ch, method, properties, body):
    try:
        msg = json.loads(body)
        data = msg.get('data', {})
        trade_id = data.get('trade_id')

        updated_trade = _update_trade_status(
            trade_id,
            TradingStatus.CANCELLED.value,
            payment_failed_at=datetime.utcnow().isoformat(),
            payment_failure_reason=data.get('reason')
        )

        if updated_trade:
            event_bus.publish(
                exchange='marketplace_exchange',
                event_type='trade.cancelled',
                data={
                    'trade_id': trade_id,
                    'status': updated_trade['status'],
                    'buyer_id': updated_trade.get('buyer_id'),
                    'seller_id': updated_trade.get('seller_id'),
                    'amount': updated_trade.get('amount'),
                    'price': updated_trade.get('price'),
                    'payment_failure_reason': updated_trade.get('payment_failure_reason'),
                    'payment_failed_at': updated_trade.get('payment_failed_at')
                }
            )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error handling payment failed event: {str(e)}")
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            pass


class TradingStatus(Enum):
    """Trading status enumerations"""
    PENDING = "pending"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'marketplace-service'}), 200


@app.route('/trades', methods=['POST'])
def create_trade():
    """
    Create a new energy trade
    Expected JSON: {
        'buyer_id': str,
        'seller_id': str,
        'amount': float,
        'price': float,
        'timestamp': str (optional)
    }
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['buyer_id', 'seller_id', 'amount', 'price']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Add timestamp if not provided
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat()
        
        # Add trade ID
        trade_id = f"TRADE_{datetime.utcnow().timestamp()}"
        data['trade_id'] = trade_id
        data['status'] = TradingStatus.PENDING.value

        trades[trade_id] = data
        trade_history.append(data)
        
        # Publish trade creation event
        event_bus.publish(
            exchange='marketplace_exchange',
            event_type='trade.created',
            data=data
        )
        
        logger.info(f"Trade created: {trade_id}")
        return jsonify({'status': 'success', 'trade_id': trade_id}), 201
    
    except Exception as e:
        logger.error(f"Error creating trade: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/trades/<trade_id>', methods=['GET'])
def get_trade(trade_id):
    """Get trade information"""
    try:
        if trade_id not in trades:
            return jsonify({'error': 'Trade not found'}), 404

        trade = trades[trade_id]
        return jsonify({
            'trade': trade
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving trade: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/trades', methods=['GET'])
def list_trades():
    """List all saved trades"""
    try:
        return jsonify({
            'count': len(trade_history),
            'trades': trade_history
        }), 200
    except Exception as e:
        logger.error(f"Error listing trades: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/market/prices', methods=['GET'])
def get_market_prices():
    """Get current market prices"""
    try:
        return jsonify({
            'prices': {
                'current': 0.12,
                'average': 0.115,
                'high': 0.15,
                'low': 0.10
            },
            'timestamp': datetime.utcnow().isoformat()
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving market prices: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Connect to event bus
    try:
        event_bus.connect()
        # subscribe to MeterDataIngested events
        event_bus.subscribe(
            exchange='energy_exchange',
            event_type='MeterDataIngested',
            callback=on_meter_data_ingested,
            queue_name='marketplace_energy_MeterDataIngested'
        )
        event_bus.subscribe(
            exchange='wallet_exchange',
            event_type='PaymentProcessed',
            callback=on_payment_processed,
            queue_name='marketplace_wallet_PaymentProcessed'
        )
        event_bus.subscribe(
            exchange='wallet_exchange',
            event_type='PaymentFailed',
            callback=on_payment_failed,
            queue_name='marketplace_wallet_PaymentFailed'
        )
        # start consuming in background thread
        t = threading.Thread(target=event_bus.start_consuming, daemon=True)
        t.start()
    except Exception as e:
        logger.error(f"Failed to connect to event bus: {str(e)}")
    
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
