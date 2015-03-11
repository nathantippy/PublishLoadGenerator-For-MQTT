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
			
		 
		 //TODO: add CSV file load for Keven this week.
		 //TODO: add telemetry for YF (must be done next week)
		 //TODO: set up VMs so we can run 4 at the same time.
		 	
		
	}
	
    
    private static void printHelp(String message) {
    	
    	//	kernel parameters in /etc/sysctl.conf in the format:
    	//		net.ipv4.tcp_tw_reuse=1
    	    	
        System.out.println(message);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("       App -b <tcp://localhost:1883> -pre <client id prefix> -q <qos num> -t <topic> -p <payloadWithoutWhitespace>");
        System.out.println("       NOTE: only the -b argument is required all others are optional.");
        System.out.println("Arguments:");
        System.out.println("          -b or -broker            URI to broker, must start with tcp://, ssl:// or local://");
        System.out.println("          -pre or -clientPrefix    prefix to add to all clients simulated by this running instance.");
        System.out.println("          -q or -qos               quality of service, can be 0, 1, or 2.");
        System.out.println("          -t or -topic             topic to send.");
        System.out.println("          -p or -payload           payload to send. White space in payload is not supported at this time.");       
        System.out.println("          -n or -number            number of concurrent piplines each simulating 4K clients");
        System.out.println("");
        System.out.println("To support more client connections to a broker instance you may want to adjust this client side setting.");
        System.out.println("    kernel parameters in /etc/sysctl.conf ");
        System.out.println("    net.ipv4.tcp_tw_reuse=1 ");
        System.out.println("");
        System.out.println();
        
    }
	
	public static void main(String[] args) {

		String server = getReqArg("-broker", "-b", args);// -b tcp://localhost:1883
		
		String clientPrefix = getOptArg("-clientPrefx", "-pre", args, "client");// -pre blue
		String topicString = getOptArg("-topic","-t",args,"thisIsATopic"); //-t topic	
		String payloadString = getOptArg("-payload","-p",args,"thisIsAPayload"); //-p helloworld //note white space is not supported in payload
		
		String qosString = getOptArg("-qos","-q",args,"0"); //-q 1		
		int qos = 0;
		try{
			qos = Integer.parseInt(qosString);
		} catch (NumberFormatException nfe) {
	        printHelp("QOS should be a number 0, 1, or 2");
	        System.exit(BAD_VALUE);
		}
			
		int maxPipes = Math.min(16, Math.max(1, Runtime.getRuntime().availableProcessors()/2));
		
		String maxPipesString = getOptArg("-number","-n",args,Integer.toString(maxPipes)); //-n 4 
		if (null!=maxPipesString) {
			try {
				maxPipes = Integer.parseInt(maxPipesString);
				if (maxPipes<1) {
					printHelp("number should be a value where  (0 < x <=16)");
			        System.exit(BAD_VALUE);
				}
			} catch (NumberFormatException nfe) {
		        printHelp("number should be a value where  (0 < x <=16)");
		        System.exit(BAD_VALUE);
			}
		}
		
		
		Main instance = new Main();	

		instance.run(server, qos, clientPrefix, topicString, payloadString, maxPipes);
		
	}

	public void run(String broker, int qos, String clientPrefix, String topicString, String payloadString, int maxPipes) {
		int clientBits = 12;//generate 4K clients per pipe
		int pipes = maxPipes;
		System.out.println("BETA 1.1");
		System.out.println("Total simulated clients: "+((1<<clientBits)*pipes)+" over "+pipes+" total concurrent pipelines" );
		
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)6,(byte)15,null, MQTTFROM.from);
				
		
		GraphManager graphManager = new GraphManager();
		
		int pipeId = pipes;
		MessageGenStage[] genStages = new MessageGenStage[pipes];
		while (--pipeId>=0) {
			genStages[pipeId] = buildSinglePipeline(messagesConfig, graphManager, clientBits, pipeId, broker, qos, clientPrefix, topicString, payloadString);
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
	

	private MessageGenStage buildSinglePipeline(RingBufferConfig messagesConfig, GraphManager graphManager, int pipeId, 
			                                    int base, String server, int qos, 
			                                    String clientPrefix, String topicString, String payloadString) {
		RingBuffer messagesRing = new RingBuffer(messagesConfig);
		assert(messagesRing.maxAvgVarLen>=256) : "messages can be blocks as big as 256";
		
		MQTTStage mStage = new MQTTStage(graphManager, messagesRing);			
		return new MessageGenStage(graphManager, messagesRing, pipeId, base, server, qos, clientPrefix, topicString, payloadString);
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

	
}
