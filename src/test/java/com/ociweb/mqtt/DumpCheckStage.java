package com.ociweb.mqtt;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DumpCheckStage extends PronghornStage{
	
	public final RingBuffer input;
	
	public DumpCheckStage(GraphManager graphManager, RingBuffer input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	@Override
	public void run() {
		
		//send one text for every fragment, just a dumb test.
		while (RingReader.tryReadFragment(input)) {
		
			int msgIdx = RingReader.getMsgIdx(input);
			if (msgIdx == MQTTFROM.MSG_MQTT_LOC) {
						    	
			        MqttMessage message = new MqttMessage();
			       
			        String payload = RingReader.readASCII(input, MQTTFROM.FIELD_PAYLOAD_LOC, new StringBuilder()).toString();
			        message.setPayload(payload.getBytes());
			        message.setRetained(false);
			        message.setQos(RingReader.readInt(input, MQTTFROM.FIELD_QOS_LOC));
			        
			        String topic = RingReader.readASCII(input, MQTTFROM.FIELD_TOPIC_LOC, new StringBuilder()).toString();
	
	//		        System.err.println(topic+" - "+payload);
			        
			
			}
			
		    RingReader.releaseReadLock(input);

		}
		
	}

}
