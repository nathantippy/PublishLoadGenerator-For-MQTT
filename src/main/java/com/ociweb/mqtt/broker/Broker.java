package com.ociweb.mqtt.broker;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.dna.mqtt.moquette.messaging.spi.impl.SimpleMessaging;
import org.dna.mqtt.moquette.server.*;
import org.dna.mqtt.moquette.server.netty.NettyAcceptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dna.mqtt.commons.Constants;

public class Broker {

	
	/**
	 * 
	 * Tasks and Goals:
	 *    TODO: A, Run all 3 applications and document where the pinch points show up with MQTT
	 *    TODO: A, Run all 3 again using web sockets.
	 *    TODO: A, Add JMX monitor data data
	 * 
	 *    TODO: A, publish any pending publish when leaving run
	 *    TODO: B, make trailing length count required to minimize conditionals.
	 *    TODO: C, add leading cursor to reduce lookup?
	 * 
	 */
	
	
	private static final Logger LOG = LoggerFactory.getLogger(Broker.class);
        	
    public static void main(String[] args) throws IOException {
        final Server server = new Server();
        
        String configPath = System.getProperty("moquette.path", null);
        server.startServer(new File(configPath, "config/moquette.conf"));
                
        LOG.info("Hacked Server started, version 0.6-SNAPSHOT base.");
        
        //Bind  a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.stopServer();
            }
        });
    }

	
}
