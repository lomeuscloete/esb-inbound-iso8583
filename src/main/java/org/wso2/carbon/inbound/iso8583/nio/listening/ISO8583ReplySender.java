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

import java.util.Iterator;

import javax.xml.namespace.QName;

import org.apache.axiom.om.OMElement;
import org.apache.axiom.soap.SOAPEnvelope;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.inbound.InboundResponseSender;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOPackager;
import org.wso2.carbon.inbound.iso8583.listening.ISO8583Constant;

/**
 * class for handle iso8583 responses.
 */
public class ISO8583ReplySender implements InboundResponseSender {

	private static final Log log = LogFactory.getLog(ISO8583ReplySender.class.getName());

	private ISO8583Session iso8583Session;

	/**
	 * keep the socket connection to send the response back to client.
	 *
	 * @param connection
	 *            created socket connection.
	 */
	public ISO8583ReplySender(ISO8583Session iso8583Session) {
		this.iso8583Session = iso8583Session;
	}

	/**
	 * Send the reply or response back to the client.
	 *
	 * @param messageContext
	 *            to get xml iso message from message context.
	 */

	public void sendBack(MessageContext messageContext) {
		log.debug("Message coming back");
		try {
			ISOPackager packager = ISO8583PackagerFactory.getPackager();
			SOAPEnvelope soapEnvelope = messageContext.getEnvelope();
			OMElement getElements = soapEnvelope.getBody().getFirstElement();
			ISOMsg isoMsg = new ISOMsg();
			isoMsg.setPackager(packager);
			Iterator<?> fields = getElements.getFirstChildWithName(new QName(ISO8583Constant.TAG_DATA))
					.getChildrenWithLocalName(ISO8583Constant.TAG_FIELD);
			while (fields.hasNext()) {
				OMElement element = (OMElement) fields.next();
				String getValue = element.getText();
				try {
					int fieldID = Integer
							.parseInt(element.getAttribute(new QName(ISO8583Constant.TAG_ID)).getAttributeValue());
					isoMsg.set(fieldID, getValue);
				} catch (NumberFormatException e) {
					log.warn("The fieldID does not contain a parsable integer" + e.getMessage(), e);
				}
			}

			byte[] msg = isoMsg.pack();
			sendResponse(msg);
			log.debug("Done");
		} catch (ISOException e) {
			handleException("Couldn't packed ISO8583 Messages", e);
		}

	}

	/**
	 * writes the packed iso message response to the client.
	 *
	 * @param responseMessage
	 *            String of packed ISO response.
	 */
	private void sendResponse(byte[] msg) {
		this.iso8583Session.send(msg);
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
