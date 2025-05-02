import paho.mqtt.client as mqtt


class MQTTPublisher:
    def __init__(self, broker_address='localhost', broker_port=1883, client_id=None):
        """
        Initialize MQTT Publisher
        :param broker_address: IP address or hostname of MQTT broker (default: 'localhost')
        :param broker_port: Port of MQTT broker (default: 1883)
        :param client_id: Client ID for this publisher (default: None - random ID will be generated)
        """
        self.broker_address = broker_address
        self.broker_port = broker_port
        self.client_id = client_id
        self.client = None
        self.connected = False

        # Callback references
        self.on_publish_callback = None
        self.on_connect_callback = None
        self.on_disconnect_callback = None

    def on_connect(self, client, userdata, flags, rc):
        """Called when the broker responds to our connection request"""
        if rc == 0:
            self.connected = True
            print(f"Connected to MQTT Broker at {self.broker_address}:{self.broker_port}")
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

    def on_publish(self, client, userdata, mid):
        """Called when a message that was to be sent using the publish() call has completed transmission"""
        print(f"Message published (mid: {mid})")
        if self.on_publish_callback:
            self.on_publish_callback(client, userdata, mid)

    def connect(self):
        """Connect to the MQTT broker"""
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish

        try:
            self.client.connect(self.broker_address, port=self.broker_port)
            # Start network loop in a separate thread
            self.client.loop_start()
        except Exception as e:
            print(f"Connection error: {e}")

    def disconnect(self):
        """Disconnect from the MQTT broker"""
        if self.client and self.connected:
            self.client.disconnect()
            self.client.loop_stop()

    def publish(self, topic, payload, qos=0, retain=False):
        """Publish a message to a topic"""
        if self.client and self.connected:
            result = self.client.publish(topic, payload, qos=qos, retain=retain)
            # Wait for publication to complete
            result.wait_for_publish()
            return result
        else:
            print("Not connected to broker. Cannot publish.")
            return None

    def set_on_publish_callback(self, callback):
        """Set a custom callback for when messages are published"""
        self.on_publish_callback = callback

    def set_on_connect_callback(self, callback):
        """Set a custom callback for when connection is established"""
        self.on_connect_callback = callback

    def set_on_disconnect_callback(self, callback):
        """Set a custom callback for when disconnection occurs"""
        self.on_disconnect_callback = callback
