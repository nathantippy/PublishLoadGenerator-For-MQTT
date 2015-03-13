package com.ociweb.mqtt;

import static org.junit.Assert.assertTrue;

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
						    	
			       		        
			        String topic = RingReader.readASCII(input, MQTTFROM.FIELD_TOPIC_LOC, new StringBuilder()).toString();
			        String payload = RingReader.readASCII(input, MQTTFROM.FIELD_PAYLOAD_LOC, new StringBuilder()).toString();
	
			        
			        assertTrue(topic.indexOf("/colors/")>0);
			        assertTrue(payload.indexOf("status")>=0);
			        
			
			}
			
		    RingReader.releaseReadLock(input);

		}
		
	}

}
