"""
Wallet Service
Handles user accounts, balance management, and transactions
"""

from flask import Flask, jsonify, request
from shared.event_bus import EventBus
import logging
from datetime import datetime
import json
import threading

logger = logging.getLogger(__name__)
app = Flask(__name__)

# Initialize event bus
event_bus = EventBus()

# In-memory store (replace with database in production)
wallets = {}


def on_trade_matched(ch, method, properties, body):
    try:
        msg = json.loads(body)
        data = msg.get('data', {})
        buyer_id = data.get('buyer_id')
        total = data.get('total_price', 0)
        # Verify wallet exists
        if buyer_id not in wallets:
            # Can't process payment for non-existing wallet
            event_bus.publish(
                exchange='wallet_exchange',
                event_type='PaymentFailed',
                data={'user_id': buyer_id, 'trade_id': data.get('trade_id'), 'reason': 'wallet_not_found', 'amount': total}
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        wallet = wallets[buyer_id]

        # Check sufficient balance
        if wallet['balance'] < total:
            # Reject payment - insufficient funds
            event_bus.publish(
                exchange='wallet_exchange',
                event_type='PaymentFailed',
                data={'user_id': buyer_id, 'trade_id': data.get('trade_id'), 'reason': 'insufficient_funds', 'amount': total, 'balance': wallet['balance']}
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Process payment (debit)
        transaction = {
            'type': 'debit',
            'amount': total,
            'description': f'Payment for {data.get("trade_id")}',
            'timestamp': datetime.utcnow().isoformat(),
            'balance_after': wallet['balance'] - total
        }
        wallet['balance'] = transaction['balance_after']
        wallet['transactions'].append(transaction)

        # Publish payment processed event and also transaction.recorded
        event_bus.publish(
            exchange='wallet_exchange',
            event_type='PaymentProcessed',
            data={'user_id': buyer_id, 'amount': total, 'trade_id': data.get('trade_id')}
        )

        event_bus.publish(
            exchange='wallet_exchange',
            event_type='transaction.recorded',
            data={'user_id': buyer_id, **transaction}
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing payment: {str(e)}")
        try:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        except Exception:
            pass


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'wallet-service'}), 200


@app.route('/wallet/create', methods=['POST'])
def create_wallet():
    """
    Create a new wallet
    Expected JSON: {
        'user_id': str,
        'initial_balance': float (optional)
    }
    """
    try:
        data = request.get_json()
        
        if 'user_id' not in data:
            return jsonify({'error': 'user_id is required'}), 400
        
        user_id = data['user_id']
        initial_balance = data.get('initial_balance', 0.0)
        
        if user_id in wallets:
            return jsonify({'error': 'Wallet already exists for this user'}), 409
        
        wallet = {
            'user_id': user_id,
            'balance': initial_balance,
            'created_at': datetime.utcnow().isoformat(),
            'transactions': []
        }
        
        wallets[user_id] = wallet
        
        # Publish wallet creation event
        event_bus.publish(
            exchange='wallet_exchange',
            event_type='wallet.created',
            data={'user_id': user_id, 'balance': initial_balance}
        )
        
        logger.info(f"Wallet created for user {user_id}")
        return jsonify({'status': 'success', 'wallet': wallet}), 201
    
    except Exception as e:
        logger.error(f"Error creating wallet: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/wallet/<user_id>', methods=['GET'])
def get_wallet(user_id):
    """Get wallet information"""
    try:
        if user_id not in wallets:
            return jsonify({'error': 'Wallet not found'}), 404
        
        wallet = wallets[user_id]
        return jsonify(wallet), 200
    except Exception as e:
        logger.error(f"Error retrieving wallet: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/wallet/<user_id>/transaction', methods=['POST'])
def record_transaction(user_id):
    """
    Record a transaction
    Expected JSON: {
        'type': str ('credit' or 'debit'),
        'amount': float,
        'description': str
    }
    """
    try:
        if user_id not in wallets:
            return jsonify({'error': 'Wallet not found'}), 404
        
        data = request.get_json()
        
        # Validate required fields
        if 'type' not in data or 'amount' not in data:
            return jsonify({'error': 'Missing required fields'}), 400
        
        if data['type'] not in ['credit', 'debit']:
            return jsonify({'error': 'Invalid transaction type'}), 400
        
        wallet = wallets[user_id]
        amount = data['amount']
        
        # Check balance for debit
        if data['type'] == 'debit' and wallet['balance'] < amount:
            return jsonify({'error': 'Insufficient balance'}), 400
        
        # Update balance
        if data['type'] == 'credit':
            wallet['balance'] += amount
        else:
            wallet['balance'] -= amount
        
        # Record transaction
        transaction = {
            'type': data['type'],
            'amount': amount,
            'description': data.get('description', ''),
            'timestamp': datetime.utcnow().isoformat(),
            'balance_after': wallet['balance']
        }
        wallet['transactions'].append(transaction)
        
        # Publish transaction event
        event_bus.publish(
            exchange='wallet_exchange',
            event_type='transaction.recorded',
            data={'user_id': user_id, **transaction}
        )
        
        logger.info(f"Transaction recorded for user {user_id}")
        return jsonify({'status': 'success', 'transaction': transaction}), 201
    
    except Exception as e:
        logger.error(f"Error recording transaction: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Connect to event bus
    try:
        event_bus.connect()
        # subscribe to TradeMatched events from marketplace
        event_bus.subscribe(
            exchange='marketplace_exchange',
            event_type='TradeMatched',
            callback=on_trade_matched,
            queue_name='wallet_marketplace_TradeMatched'
        )
        t = threading.Thread(target=event_bus.start_consuming, daemon=True)
        t.start()
    except Exception as e:
        logger.error(f"Failed to connect to event bus: {str(e)}")
    
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
