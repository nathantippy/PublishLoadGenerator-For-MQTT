package com.ociweb.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.threading.GraphManager;

public class MQTTStage extends PronghornStage {
	
	MqttClient client; 
	RingBuffer input;
	
	protected MQTTStage(GraphManager graphManager, RingBuffer input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	
	@Override
	public void startup() {
		super.startup();
		try {
			client = new MqttClient("tcp://localhost:1883", "pahomqttpublish1");
			client.connect();
		} catch (MqttException e) {
			throw new RuntimeException(e);
		}
	}




	@Override
	public void run() {
		//send one text for every fragment, just a dumb test.
		while (RingReader.tryReadFragment(input)) {
		
		    try {
		        MqttMessage message = new MqttMessage();
		        message.setPayload("A single message".getBytes());
		        client.publish("pahodemo/test", message);
		      } catch (MqttException e) {
		        e.printStackTrace();
		      }
		}
	}
	
	@Override
	public void shutdown() {
		super.shutdown();
		try {
			client.disconnect();
		} catch (MqttException e) {
			throw new RuntimeException(e);
		}
	}

}
