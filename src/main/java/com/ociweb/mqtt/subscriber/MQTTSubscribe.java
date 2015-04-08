package com.ociweb.mqtt.subscriber;

import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.ring.FieldReferenceOffsetManager.lookupTemplateLocator;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.dna.mqtt.moquette.server.Server;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;


public class MQTTSubscribe extends PronghornStage {
	
	protected static final int QOS_BITS = 2;
	protected static final int RET_BITS = 1;
	protected static final int DUP_BITS = 1;
	
	protected static final int QOS_MASK = (1<<QOS_BITS)-1;
	protected static final int RET_MASK = (1<<RET_BITS)-1;
	protected static final int DUP_MASK = (1<<DUP_BITS)-1;
	
	protected static final int DUP_SHIFT = 0;
	protected static final int RET_SHIFT = DUP_SHIFT+DUP_BITS;
	protected static final int QOS_SHIFT = RET_SHIFT+RET_BITS;



	private static Logger log = LoggerFactory.getLogger(MQTTSubscribe.class);
	
	private final Properties configProperties;
	private MqttClient client;
	private MqttConnectOptions connOptions;
	private final RingBuffer outputRing;
	
	private final FieldReferenceOffsetManager FROM;
	private final int MSG_MQTT;
	private final int FIELD_TOPIC;
	private final int FIELD_PAYLOAD;
	private final int FIELD_META_MASK;

	
	
	public MQTTSubscribe(GraphManager gm, RingBuffer outputRing, Properties configProperties) {
		super(gm,NONE,outputRing);
		
		this.configProperties = configProperties;
		this.outputRing = outputRing;
		
		this.FROM = RingBuffer.from(outputRing);
		
		MSG_MQTT = lookupTemplateLocator("MQTT",FROM);  
		
		FIELD_TOPIC = lookupFieldLocator("Topic", MSG_MQTT, FROM);
		FIELD_PAYLOAD = lookupFieldLocator("Payload", MSG_MQTT, FROM);
		FIELD_META_MASK = lookupFieldLocator("MetaMask", MSG_MQTT, FROM);
	}


	@Override
	public void startup() {
		try {

			
			
			
//This is an alternate approach but it is using call backs under the covers			
//			//https://github.com/fusesource/mqtt-client
//			MQTT mqtt = new MQTT();
//			mqtt.setHost("localhost", 1883);
//			BlockingConnection connection = mqtt.blockingConnection();
//			connection.connect();
//          Topic[] topics = {new Topic("foo", QoS.AT_LEAST_ONCE)};
//			byte[] qoses = connection.subscribe(topics);		
//			
//			Message message = connection.receive();
//			System.out.println(message.getTopic());
//			byte[] payload = message.getPayload();
//			// process the message then:
//			message.ack();
//          review use of setDispatchQueue		
			//			<dependency>
			//			  <groupId>org.fusesource.mqtt-client</groupId>
			//			  <artifactId>mqtt-client</artifactId>
			//			  <version>1.10</version>
			//			</dependency>
			
			
			//Paho code below here
			
			
			connOptions = new MqttConnectOptions();
			connOptions.setCleanSession(true);
		//	connOptions.setKeepAliveInterval(0);
		//	connOptions.setConnectionTimeout(0);
	
			client = new MqttClient("tcp://localhost:1883", "TestClient", new MemoryPersistence());
			
			client.setCallback(new MqttCallback(){

				@Override
				public void connectionLost(Throwable cause) {
					log.error("no support for connection lost",cause);
					RingBuffer.shutdown(outputRing);
				}

				//TODO: D, topic should be a constant constructed with the ring buffer
				//      if we use 1 ring buffer per subscription per topic
				
				@Override
				public void messageArrived(String topic, MqttMessage message) throws Exception {
					//To support QoS on the server side this method MUST NOT return
					//until after the message is written to the ring buffer and safe.
										
					int metaMask = ((QOS_MASK & message.getQos()) << QOS_SHIFT) |
							       ((message.isRetained() ? 1 : 0) << RET_SHIFT) |
							       ((message.isDuplicate() ? 1 : 0) << DUP_SHIFT);
					
					String top = topic;
					
					byte[] payload = message.getPayload();
					
					
					RingWriter.blockWriteFragment(outputRing, MSG_MQTT);
					
					RingWriter.writeASCII(outputRing, FIELD_TOPIC, topic);
					RingWriter.writeBytes(outputRing, FIELD_PAYLOAD, payload);
					RingWriter.writeInt(outputRing, FIELD_META_MASK, metaMask);
					
					RingWriter.publishWrites(outputRing);
					
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
					throw new UnsupportedOperationException();
				}});
			
			System.err.println("subscribed");
			client.connect(connOptions);     
			//client.setTimeToWait(-1);
			client.subscribe("#", 0);//"/root", 0);
			
			
		} catch (MqttException e) {
			throw new RuntimeException(e);
		}
		
		
	}
	
	@Override
	public void run() {
		//Must not be scheduled or do any work, input server is putting data on the output ring as needed with its own thread
		//TODO: D, Build a no thread, skip annotation so this only gets the shutdown and does no work.
		Thread.yield();
		//annotate this as a no-thread scheduler.
	}

	@Override
	public void shutdown() {
		try {
			client.disconnect();
		} catch (MqttException e) {
			throw new RuntimeException(e);
		}

	}

	
}
