package com.ociweb.mqtt;

import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.threading.GraphManager;
import com.ociweb.pronghorn.stage.threading.StageManager;
import com.ociweb.pronghorn.stage.threading.ThreadPerStageManager;

public class Main {

	public Main() {
		
		 //TODO: must be big enought for 256
		 //TODO: must extract to function to write multiple at once.
		 //TODO: must scale up to many.
		 //TODO: must use custom template.xml instead of raw bytes.
		 	
		
	}
	
	
	public static void main(String[] args) {

		Main instance = new Main();
		
		instance.run();
		
	}

	public void run() {
		
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)8,(byte)21,null, FieldReferenceOffsetManager.RAW_BYTES);
		RingBuffer messagesRing = new RingBuffer(messagesConfig);

		GraphManager graphManager = new GraphManager();
		
		MQTTStage mStage = new MQTTStage(graphManager, messagesRing);		
		MessageGenStage  genStage = new MessageGenStage(graphManager, messagesRing);
		
		StageManager scheduler = new ThreadPerStageManager(GraphManager.cloneAll(graphManager));
		scheduler.startup();
		 
		 
		long TIMEOUT_SECONDS = 120;
		boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		
	}
	
}
