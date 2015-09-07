package com.ociweb.mqttTestTools.publisher;

import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupFieldLocator;
import static com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager.lookupTemplateLocator;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.schema.loader.TemplateHandler;

public class MQTTFROM {

	public static final FieldReferenceOffsetManager from;
	
	public static final int MSG_MQTT_LOC;
	public static final int FIELD_SERVER_URI_LOC;
	public static final int FIELD_CLIENT_ID_LOC;
	public static final int FIELD_CLIENT_INDEX_LOC;	
	public static final int FIELD_TOPIC_LOC;
	public static final int FIELD_PAYLOAD_LOC;
	public static final int FIELD_QOS_LOC;
	
	
	static {
		
		FieldReferenceOffsetManager temp = null;
		try {
			temp = TemplateHandler.loadFrom("/mqttTemplate.xml");
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		from = temp;
		
		MSG_MQTT_LOC = lookupTemplateLocator("MQTT",from);  
		FIELD_SERVER_URI_LOC = lookupFieldLocator("serverURI", MSG_MQTT_LOC, from);
		FIELD_CLIENT_ID_LOC = lookupFieldLocator("clientid", MSG_MQTT_LOC, from);
		FIELD_CLIENT_INDEX_LOC = lookupFieldLocator("index", MSG_MQTT_LOC, from);		
		FIELD_TOPIC_LOC = lookupFieldLocator("topic", MSG_MQTT_LOC, from);
		FIELD_PAYLOAD_LOC = lookupFieldLocator("payload", MSG_MQTT_LOC, from);
		FIELD_QOS_LOC = lookupFieldLocator("qos", MSG_MQTT_LOC, from);
		
	}
}
