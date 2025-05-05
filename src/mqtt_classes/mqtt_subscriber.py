import paho.mqtt.client as mqtt


class MQTTSubscriber:
    def __init__(self, broker_address='localhost', broker_port=1883, client_id=None, topic=None):
        """
        Initialize MQTT Subscriber
        :param broker_address: IP address or hostname of MQTT broker (default: 'localhost')
        :param broker_port: Port of MQTT broker (default: 1883)
        :param client_id: Client ID for this subscriber (default: None - random ID will be generated)
        :param topic: Topic to subscribe to (default: None)
        """
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id = client_id
        self.topic = topic
        self.client = None
        self.connected = False
        self.subscribed = False
        self.received_messages = []

        self.on_message_callback = None
        self.on_connect_callback = None
        self.on_disconnect_callback = None

    def on_connect(self, client, userdata, flags, rc):
        """Called when the broker responds to our connection request"""
        if rc == 0:
            self.connected = True
            print(f"A Subscriber Is Connected to MQTT Broker at {self.broker_address}:{self.broker_port}")
            if self.topic:
                self.subscribe(self.topic)
            if self.on_connect_callback:
                self.on_connect_callback(client, userdata, flags, rc)
        else:
            print(f"Connection failed with result code {rc}")

    def on_disconnect(self, client, userdata, rc):
        """Called when the client disconnects from the broker"""
        self.connected = False
        print(f"Disconnected from MQTT Broker (rc: {rc})")
        if self.on_disconnect_callback:
            self.on_disconnect_callback(client, userdata, rc)

    def on_message(self, client, userdata, message):
        """Called when a message has been received on a topic that the client subscribes to"""
        msg = {
            "topic": message.topic,
            "payload": message.payload.decode("utf-8"),
            "qos": message.qos,
            "retain": message.retain
        }
        self.received_messages.append(msg)
        # print(f"Received message on topic {msg['topic']}: {msg['payload']}")
        if self.on_message_callback:
            self.on_message_callback(client, userdata, message)

    def connect(self):
        """Connect to the MQTT broker"""
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        try:
            self.client.connect(self.broker_address, port=self.broker_port)
            self.client.loop_start()
        except Exception as e:
            print(f"Connection error: {e}")

    def disconnect(self):
        """Disconnect from the MQTT broker"""
        if self.client and self.connected:
            self.client.disconnect()
            self.client.loop_stop()

    def subscribe(self, topic, qos=0):
        """Subscribe to a topic"""
        if self.client and self.connected:
            self.client.subscribe(topic, qos=qos)
            self.subscribed = True
            print(f"Subscribed to topic: {topic}")
        else:
            print("Not connected to broker. Cannot subscribe.")

    def set_on_message_callback(self, callback):
        """Set a custom callback for when messages are received"""
        self.on_message_callback = callback

    def set_on_connect_callback(self, callback):
        """Set a custom callback for when connection is established"""
        self.on_connect_callback = callback

    def set_on_disconnect_callback(self, callback):
        """Set a custom callback for when disconnection occurs"""
        self.on_disconnect_callback = callback

    def get_received_messages(self):
        """Get all received messages"""
        return self.received_messages

    def clear_received_messages(self):
        """Clear the received messages buffer"""
        self.received_messages = []
