/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.carbon.inbound.iso8583.nio.listening;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.SynapseException;
import org.apache.synapse.inbound.InboundProcessorParams;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOPackager;

/**
 * Class for handling the iso message request.
 */
public class ISO8583MessageRequestHandler implements Runnable {
	private static final Log log = LogFactory.getLog(ISO8583MessageRequestHandler.class);
	//private TCPConnection connection;
	private byte[] message;
	private ISOPackager packager;
	private ISO8583MessageInject msgInject;


	public ISO8583MessageRequestHandler(byte[] message, ISO8583Session iso8583Session, InboundProcessorParams params) {
		
		//this.connection = connection;
		this.message = message;
		this.packager = ISO8583PackagerFactory.getPackager();
		this.msgInject = new ISO8583MessageInject(params,iso8583Session);
	
	}

	/**
	 * connect method for read the request from inputStreamReader and inject
	 * into sequence.
	 */
	public void handleIncommingMessage()  {
		
		if (message != null) {
			
			
			if (log.isDebugEnabled()) {
				String messageStr = "";
				if (message != null) {
					for (int i = 0; i < message.length; i++) {
						messageStr += "0x" + Integer.toHexString(message[i]) + ",";
					}
				}
				System.out.println("Message: " + messageStr);
			}
			ISOMsg isoMessage = unpackRequest(message);
			if(log.isDebugEnabled()){
				log.debug("Message: " + isoMessage);
			}
			msgInject.inject(isoMessage);
		}
	}

	public void run() {
		
			handleIncommingMessage();
		
	}

	/**
	 * unpack the string iso message to obtain its fields.
	 *
	 * @param message
	 *            String ISOMessage
	 */
	private ISOMsg unpackRequest(byte[] message) {
		ISOMsg isoMsg = null;
		try {
			isoMsg = new ISOMsg();
			isoMsg.setPackager(packager);
			isoMsg.unpack(message);
		} catch (ISOException e) {
			handleISOException(new String(message), e);
		}
		return isoMsg;
	}

	/**
	 * handle the ISOMessage which is not in the ISO Standard.
	 *
	 * @param message
	 *            String ISOMessage
	 */
	private void handleISOException(String message, ISOException e) {
		
		handleException("Couldn't unpack the message since financial message is not in ISO Standard", e);
		
	}

	/**
	 * handle the Exception
	 *
	 * @param msg
	 *            error message
	 * @param e
	 *            an Exception
	 */
	private void handleException(String msg, Exception e) {
		log.error(msg, e);
		throw new SynapseException(msg);
	}
}
