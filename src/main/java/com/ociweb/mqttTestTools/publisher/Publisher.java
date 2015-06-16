package com.ociweb.mqttTestTools.publisher;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class Publisher {

	private static final int BAD_VALUE = -3;
    private static final int MISSING_REQ_ARG = -2;
    private static final int MISSING_ARG_VALUE = -1;
    
    private static final int CLIENTS_PER_PIPE_BITS = 12;//generate 4K clients per pipe
    
    //To start up a broker
    // mosquitto_sub -t "#" -v -k 3
    //
	public Publisher() {
	}
	
    
    private static void printHelp(String message) {
    	
    	//	kernel parameters in /etc/sysctl.conf in the format:
    	//		net.ipv4.tcp_tw_reuse=1
    	    	
        System.out.println(message);
        System.out.println();
        System.out.println("Usage:");
        System.out.println("       java -jar MQTTSim.jar -b <tcp://localhost:1883> -pre <client id prefix> -q <qos num> -t <topic> -p <payloadWithoutWhitespace>");
        System.out.println("       NOTE: only the -b argument is required all others are optional.");
        System.out.println("Arguments:");
        System.out.println("          -b or -broker            URI to broker, must start with tcp://, ssl:// or local://");
        System.out.println("          -pre or -clientPrefix    prefix to add to all simulated clients. Default: client");
        System.out.println("          -q or -qos               quality of service, can be 0, 1, or 2.");
        System.out.println("          -t or -topic             topic to send.");
        System.out.println("          -p or -payload           payload to send. White space in payload is not supported at this time.");       
        System.out.println("          -n or -number            number of concurrent piplines each simulating 4K clients");
        System.out.println("          -c or -csv               path to CSV file that contains  QoS,topic,payload  (QoS is a number 0, 1, or  2)");
        
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
		
		
		String csvString = getOptArg("-csv","-c",args, null); //-c <path to csv content file>	
		MappedByteBuffer csvData = null;
		if (null!=csvString) {			
				try {
					FileChannel fileChannel = new RandomAccessFile(new File(csvString), "r").getChannel();
					csvData = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
				} catch (Exception e) {
					throw new RuntimeException(e);
				} 
			
		
			
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
		
		System.out.println("BETA 1.2");
		System.out.println("Total simulated clients: "+((1<<CLIENTS_PER_PIPE_BITS)*maxPipes)+" over "+maxPipes+" total concurrent pipelines" );
		
		Publisher instance = new Publisher();	

		if (null!=csvData) {
		    //run from csv file data
			instance.run(server, clientPrefix, maxPipes, csvData);
		} else {
			//normal run with fixed single message
			instance.run(server, clientPrefix, maxPipes, qos, topicString, payloadString);
		}
	}

	private void run(String broker, String clientPrefix, int maxPipes,	MappedByteBuffer csvData) {

			RingBufferConfig messagesConfig = new RingBufferConfig((byte)6,(byte)15,null, MQTTFROM.from);
			RingBufferConfig linesConfig = new RingBufferConfig((byte)6,(byte)15,null, FieldReferenceOffsetManager.RAW_BYTES );
			
			GraphManager graphManager = new GraphManager();
					
			int pipeId = maxPipes;
			MQTTStage[] outputStages = new MQTTStage[maxPipes];
			while (--pipeId>=0) {
	
				RingBuffer linesRing = new RingBuffer(linesConfig);
				RingBuffer messagesRing = new RingBuffer(messagesConfig);
				
				outputStages[pipeId] = new MQTTStage(graphManager, messagesRing);
				
				MessageCSVStage messageCSVStage = new MessageCSVStage(graphManager, linesRing, messagesRing, CLIENTS_PER_PIPE_BITS, pipeId, broker, clientPrefix);
				
				LineSplitterByteBufferStage lineStage = new LineSplitterByteBufferStage(graphManager, csvData.duplicate(), linesRing);
				
			}
					
			run(maxPipes, graphManager, outputStages);
		
	}


	public void run(String broker, String clientPrefix, final int maxPipes, int qos, String topicString, String payloadString) {
			
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)6,(byte)15,null, MQTTFROM.from);
						
		GraphManager graphManager = new GraphManager();
		
		int pipeId = maxPipes;
		MQTTStage[] outputStages = new MQTTStage[maxPipes];
		while (--pipeId>=0) {
			RingBuffer messagesRing = new RingBuffer(messagesConfig);
			outputStages[pipeId] = new MQTTStage(graphManager, messagesRing);
			MessageGenStage messageGenStage = new MessageGenStage(graphManager, messagesRing, CLIENTS_PER_PIPE_BITS, pipeId, broker, qos, clientPrefix, topicString, payloadString);
		}
				
		run(maxPipes, graphManager, outputStages);
		
	}


	private void run(final int maxPipes, GraphManager graphManager,	MQTTStage[] outputStages) {
		
		//Add monitoring
		//MonitorConsoleStage.attach(graphManager);
				
		//Enable batching
		//GraphManager.enableBatching(graphManager);
		
		StageScheduler scheduler = new ThreadPerStageScheduler(graphManager);
		
		long start = System.currentTimeMillis();
		scheduler.startup();		 
		 
		Scanner scan = new Scanner(System.in);
		System.out.println("press enter to exit");
		scan.hasNextLine();
		
		System.out.println("exiting...");
		scheduler.shutdown();
				
		long TIMEOUT_SECONDS = 10;
		scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);

		long duration = System.currentTimeMillis() - start;
		
		showTotals(maxPipes, outputStages, duration);
	}


	private void showTotals(int pipes, MQTTStage[] genStages, long duration) {
		
		int i;
		long messages = 0;
		i = genStages.length;
		while (--i>=0) {
			messages += genStages[i].getMessageCount();
		}		
		
		float msgPerMs = (pipes*messages)/(float)duration;
		
		System.out.println("msg/ms "+msgPerMs+"  totalMessages:"+messages);
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
