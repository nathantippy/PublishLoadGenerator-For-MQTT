package com.ociweb.mqtt;

import static org.junit.Assert.assertTrue;

import com.ociweb.mqttTestTools.publisher.MQTTFROM;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class DumpCheckStage extends PronghornStage{
	
	public final Pipe input;
	
	public DumpCheckStage(GraphManager graphManager, Pipe input) {
		super(graphManager, input, NONE);
		this.input = input;
	}

	@Override
	public void run() {
		
		//send one text for every fragment, just a dumb test.
		while (PipeReader.tryReadFragment(input)) {
		
			int msgIdx = PipeReader.getMsgIdx(input);
			if (msgIdx == MQTTFROM.MSG_MQTT_LOC) {
						    	
			       		        
			        String topic = PipeReader.readASCII(input, MQTTFROM.FIELD_TOPIC_LOC, new StringBuilder()).toString();
			        String payload = PipeReader.readASCII(input, MQTTFROM.FIELD_PAYLOAD_LOC, new StringBuilder()).toString();
	
			        
			        assertTrue(topic.indexOf("/colors/")>0);
			        assertTrue(payload.indexOf("status")>=0);
			        
			
			}
			
		    PipeReader.releaseReadLock(input);

		}
		
	}

}
