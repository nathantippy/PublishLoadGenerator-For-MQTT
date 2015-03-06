package com.ociweb.mqtt;

import java.util.Scanner;
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
		
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)8,(byte)21,null, MQTTFROM.from);
		
		

		GraphManager graphManager = new GraphManager();
		
		int clientBits = 10;//generate 1K clients per pipe
		int pipes = 4;
		
		int i = pipes;
		MessageGenStage[] genStages = new MessageGenStage[pipes];
		while (--i>=0) {
			genStages[i] = buildSinglePipeline(messagesConfig, graphManager, clientBits, i);
		}
				
		StageManager scheduler = new ThreadPerStageManager(GraphManager.cloneAll(graphManager));
		
		long start = System.currentTimeMillis();
		scheduler.startup();		 
		 
		Scanner scan = new Scanner(System.in);
		System.out.println("press enter to exit");
		scan.hasNextLine();
		
		System.out.println("exiting...");
		scheduler.shutdown();
		
		
		long TIMEOUT_SECONDS = 20;
		boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		if (!cleanExit) {
			System.err.println("clean exit:"+cleanExit+" unable to get totals.");
			return;
		}
		
		long duration = System.currentTimeMillis() - start;
		
		showTotals(pipes, genStages, duration);
		
	}


	private void showTotals(int pipes, MessageGenStage[] genStages,
			long duration) {
		int i;
		long messages = 0;
		i = genStages.length;
		while (--i>=0) {
			messages += genStages[i].getMessageCount();
		}		
		
		float msgPerMs = (pipes*messages)/(float)duration;
		
		System.err.println("msg/ms "+msgPerMs+"  totalMessages:"+messages);
	}


	private MessageGenStage buildSinglePipeline(RingBufferConfig messagesConfig, GraphManager graphManager, int clientBits, int base) {
		RingBuffer messagesRing = new RingBuffer(messagesConfig);
		MQTTStage mStage = new MQTTStage(graphManager, messagesRing);			
		return new MessageGenStage(graphManager, messagesRing, clientBits, base);
	}
	
}
