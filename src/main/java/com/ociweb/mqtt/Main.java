package com.ociweb.mqtt;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class Main {

	private static final int BAD_VALUE = -3;
    private static final int MISSING_REQ_ARG = -2;
    private static final int MISSING_ARG_VALUE = -1;
    
	public Main() {
			
		 
		 //TODO: add palyoud and topic args.
		 //TODO: add CSV file load
		 //TODO: add telemetry for YF (must be done next week)
		 	
		
	}
	
	
	public static void main(String[] args) {

		String server = getReqArg("-broker", "-b", args);// -b tcp://localhost:1883
		String clientPrefix = getReqArg("-clientPrefx", "-p", args);// -p blue
		String qosString = getOptArg("-qos","-q",args,"0"); //-q 1
		int qos = 0;
		try{
			qos = Integer.parseInt(qosString);
		} catch (NumberFormatException nfe) {
	        printHelp("QOS should be a number 0, 1, or 2");
	        System.exit(BAD_VALUE);
		}
		
		
		
		Main instance = new Main();
		
		int clientBits = 12;//generate 4K clients per pipe
		int pipes = Math.min(6, Math.max(1, Runtime.getRuntime().availableProcessors()));		
		
		System.out.println("Total simulated clients: "+((1<<clientBits)*pipes)+" over "+pipes+" total threads" );
		instance.run(server, clientBits, pipes, qos, clientPrefix);
		
	}

	public void run(String broker, int clientBits, int pipes, int qos, String clientPrefix) {
	
		
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)6,(byte)15,null, MQTTFROM.from);
				
		
		GraphManager graphManager = new GraphManager();
		
		int i = pipes;
		MessageGenStage[] genStages = new MessageGenStage[pipes];
		while (--i>=0) {
			genStages[i] = buildSinglePipeline(messagesConfig, graphManager, clientBits, i, broker, qos, clientPrefix);
		}
				
		StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(graphManager));
		
		long start = System.currentTimeMillis();
		scheduler.startup();		 
		 
		Scanner scan = new Scanner(System.in);
		System.out.println("press enter to exit");
		scan.hasNextLine();
		
		System.out.println("exiting...");
		scheduler.shutdown();
		
		
		long TIMEOUT_SECONDS = 40;
		boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		if (!cleanExit) {
			System.out.println("Totals are estimates becuase some messages in flight were lost while shutting down.");
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
		
		System.out.println("msg/ms "+msgPerMs+"  totalMessages:"+messages);
	}
	

	private MessageGenStage buildSinglePipeline(RingBufferConfig messagesConfig, GraphManager graphManager, int clientBits, int base, String server, int qos, String clientPrefix) {
		RingBuffer messagesRing = new RingBuffer(messagesConfig);
		assert(messagesRing.maxAvgVarLen>=256) : "messages can be blocks as big as 256";
		
		MQTTStage mStage = new MQTTStage(graphManager, messagesRing);			
		return new MessageGenStage(graphManager, messagesRing, clientBits, base, server, qos, clientPrefix);
	}
	
    private static String getReqArg(String longName, String shortName, String[] args) {
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    printHelp("Expected value not found");
                    System.exit(MISSING_ARG_VALUE);
                }
                return token.trim();
            }
            prev = token;
        }
        printHelp("Expected value not found");
        System.exit(MISSING_REQ_ARG);
        return null;
    }
    
    private static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    return defaultValue;
                }
                return token.trim();
            }
            prev = token;
        }
        return defaultValue;
    }
    
    private static void printHelp(String message) {
        System.out.println(message);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("       App -broker <tcp://localhost:1883> -p <client id prefix> -c <csv file>");
        System.out.println();
        System.out.println();
    }
	
}
