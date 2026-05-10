"""
Event Bus Module
Handles inter-service communication through a message broker
"""

import pika
import json
import logging
import os
import time
from typing import Callable, Any
from functools import wraps

logger = logging.getLogger(__name__)


class EventBus:
    """Event bus for publish-subscribe communication between services"""
    
    def __init__(self, rabbitmq_url: str = 'amqp://guest:guest@localhost:5672/'):
        """Initialize the Event Bus with RabbitMQ connection"""
        self.rabbitmq_url = os.getenv('RABBITMQ_URL', rabbitmq_url)
        self.connection = None
        self.channel = None
        self.subscribers = {}

    def _is_connection_alive(self) -> bool:
        return bool(
            self.connection
            and not self.connection.is_closed
            and self.channel
            and not self.channel.is_closed
        )

    def ensure_connection(self):
        """Reconnect if the RabbitMQ channel or connection was closed."""
        if self._is_connection_alive():
            return
        self.connection = None
        self.channel = None
        self.connect()
        
    def connect(self):
        """Establish connection to RabbitMQ"""
        last_error = None
        for attempt in range(10):
            try:
                self.connection = pika.BlockingConnection(
                    pika.URLParameters(self.rabbitmq_url)
                )
                self.channel = self.connection.channel()
                logger.info("Connected to RabbitMQ")
                return
            except Exception as e:
                last_error = e
                logger.warning(
                    "Failed to connect to RabbitMQ (attempt %s/10): %s",
                    attempt + 1,
                    str(e),
                )
                time.sleep(2)

        logger.error(f"Failed to connect to RabbitMQ: {str(last_error)}")
        raise last_error
    
    def disconnect(self):
        """Close RabbitMQ connection"""
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("Disconnected from RabbitMQ")
    
    def publish(self, exchange: str, event_type: str, data: Any):
        """Publish an event to the message broker"""
        # Use a short-lived connection/channel for publishing so publishers
        # from different threads (Flask handlers) don't interfere with the
        # consumer connection used by start_consuming.
        try:
            conn = pika.BlockingConnection(pika.URLParameters(self.rabbitmq_url))
            ch = conn.channel()
            ch.exchange_declare(exchange=exchange, exchange_type='topic', durable=True)
            message = json.dumps({'event_type': event_type, 'data': data})
            ch.basic_publish(
                exchange=exchange,
                routing_key=event_type,
                body=message,
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=pika.DeliveryMode.Persistent
                )
            )
            logger.info(f"Published event: {event_type}")
            try:
                ch.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Failed to publish event: {str(e)}")
            raise
    
    def subscribe(self, exchange: str, event_type: str, callback: Callable, queue_name: str | None = None):
        """Subscribe to an event type using a stable queue name when provided."""
        self.ensure_connection()
        
        try:
            # Declare exchange
            self.channel.exchange_declare(
                exchange=exchange,
                exchange_type='topic',
                durable=True
            )
            
            # Declare queue
            if queue_name is None:
                queue_name = f"{exchange}_{event_type}"
            result = self.channel.queue_declare(
                queue=queue_name,
                durable=True
            )
            queue_name = result.method.queue
            
            # Bind queue to exchange
            self.channel.queue_bind(
                exchange=exchange,
                queue=queue_name,
                routing_key=event_type
            )
            
            # Set up consumer
            self.channel.basic_consume(
                queue=queue_name,
                on_message_callback=callback,
                auto_ack=False
            )
            
            logger.info(f"Subscribed to event: {event_type}")
        except Exception as e:
            logger.error(f"Failed to subscribe to event: {str(e)}")
            raise
    
    def start_consuming(self):
        """Start consuming messages"""
        if not self.channel:
            raise RuntimeError("Not connected to message broker")
        
        try:
            logger.info("Starting to consume messages...")
            self.channel.start_consuming()
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            raise
