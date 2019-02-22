package org.wso2.carbon.inbound.iso8583.nio.listening;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.inbound.InboundProcessorParams;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericInboundListener;
import org.wso2.carbon.inbound.iso8583.listening.ISO8583Constant;



public class ISO8583MessageConsumer extends GenericInboundListener {
	private static final Log log = LogFactory.getLog(ISO8583MessageConsumer.class);
	public InboundProcessorParams params;
	private int port;
	private ISO8583MessageConnection messageConnection;

	public ISO8583MessageConsumer(InboundProcessorParams params) {	
		super(params);
		log.debug("ISO8583MessageConsumer created");
		Properties properties = params.getProperties();
		this.params = params;
		try {
			this.port = Integer.parseInt(properties.getProperty(ISO8583Constant.PORT));
		} catch (NumberFormatException e) {
			handleException("The String does not contain a parsable integer", e);
		}
		this.messageConnection = new ISO8583MessageConnection(port, params);
	}

	@Override
	public void init() {
		messageConnection.start();

	}

	@Override
	public void destroy() {
		messageConnection.destroyConnection();;

	}

}
