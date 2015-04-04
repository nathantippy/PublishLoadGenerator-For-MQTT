package com.ociweb.mqtt.subscriber;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.ConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorConsoleStage;
import com.ociweb.pronghorn.stage.monitor.MonitorFROM;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class Subscriber {

	private static final long TIMEOUT_SECONDS = 3;
	private static FieldReferenceOffsetManager from;
	private static int maxLengthVarField = 50;
	public static final int messagesOnRing = 1<<4;
	public static final int monitorMessagesOnRing = 7;
	private static int testInSeconds = 210;
	private static final Integer consoleRate = Integer.valueOf(1000000000);
	
	//Target msg per second on single socent
	//add telemetry to JMS and mointor where the pinch points are.
	
	
	public static void main(String[] args) {
		RingBufferConfig ringBufferConfig;
		
		try {
			
			from = TemplateHandler.loadFrom("/mqttSubscriber.xml");
			ringBufferConfig = new RingBufferConfig(from, messagesOnRing, maxLengthVarField);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		GraphManager gm = new GraphManager();
		RingBuffer ringBuffer = new RingBuffer(ringBufferConfig);
		Properties props = new Properties();
		props.setProperty("port", "1883");
		props.setProperty("host", "0.0.0.0");
		props.setProperty("password_file","./temp.txt");// "../broker/config/password_file.conf");
		
		MQTTSubscribe broker = new MQTTSubscribe(gm, ringBuffer, props);		
		ConsoleStage console = new ConsoleStage(gm, ringBuffer);
		
		GraphManager.addAnnotation(gm, GraphManager.SCHEDULE_RATE, consoleRate , console );
		
		MonitorConsoleStage.attach(gm);
				
		GraphManager.enableBatching(gm);
		
		StageScheduler scheduler = new ThreadPerStageScheduler(gm);
		 
	    long startTime = System.currentTimeMillis();
		scheduler.startup();

		try {
			
			Thread.sleep(testInSeconds *1000);
		} catch (InterruptedException e) {
		}
		
		//NOTE: if the tested input stage is the sort of stage that calls shutdown on its own then
		//      you do not need the above sleep
		//      you do not need the below shutdown
		//
		scheduler.shutdown();
		
        boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
 
        long duration = System.currentTimeMillis()-startTime;
        
        
        float messages = console.totalMessages();
        float ms = duration;
        System.err.println((messages/ms)+"msg/ms   "+messages+" msg ");
        
        
        
	}

}
