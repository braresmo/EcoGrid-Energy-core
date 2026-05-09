"""
Meter Service
Handles energy meter data collection and publishing
"""

from flask import Flask, jsonify, request
from shared.event_bus import EventBus
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)
app = Flask(__name__)

# Initialize event bus
event_bus = EventBus()

# In-memory store for meter readings
meter_readings = {}
reading_history = []


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'service': 'meter-service'}), 200


@app.route('/meter/reading', methods=['POST'])
def submit_meter_reading():
    """
    Submit a new meter reading
    Expected JSON: {
        'meter_id': str,
        'timestamp': str,
        'consumption': float,
        'location': str
    }
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        required_fields = ['meter_id', 'consumption']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Add timestamp if not provided
        if 'timestamp' not in data:
            data['timestamp'] = datetime.utcnow().isoformat()

        meter_id = data['meter_id']
        meter_readings[meter_id] = data
        reading_history.append(data)
        
        # Publish event
        event_bus.publish(
            exchange='energy_exchange',
            event_type='meter.reading.updated',
            data=data
        )
        # Also publish a higher-level event used by other services
        try:
            event_bus.publish(
                exchange='energy_exchange',
                event_type='MeterDataIngested',
                data=data
            )
        except Exception:
            # don't fail the request if secondary publish fails
            logger.warning('Failed to publish MeterDataIngested event')
        
        logger.info(f"Meter reading recorded for meter {data['meter_id']}")
        return jsonify({'status': 'success', 'message': 'Meter reading recorded'}), 201
    
    except Exception as e:
        logger.error(f"Error submitting meter reading: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/meter/<meter_id>', methods=['GET'])
def get_meter_info(meter_id):
    """Get meter information"""
    try:
        if meter_id not in meter_readings:
            return jsonify({'error': 'Meter not found'}), 404

        reading = meter_readings[meter_id]
        return jsonify({
            'meter_id': meter_id,
            'status': 'active',
            'last_reading': reading
        }), 200
    except Exception as e:
        logger.error(f"Error retrieving meter info: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/meter', methods=['GET'])
def list_meter_readings():
    """List all saved meter readings"""
    try:
        return jsonify({
            'count': len(reading_history),
            'readings': reading_history
        }), 200
    except Exception as e:
        logger.error(f"Error listing meter readings: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Connect to event bus
    try:
        event_bus.connect()
    except Exception as e:
        logger.error(f"Failed to connect to event bus: {str(e)}")
    
    # Run Flask app
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)
