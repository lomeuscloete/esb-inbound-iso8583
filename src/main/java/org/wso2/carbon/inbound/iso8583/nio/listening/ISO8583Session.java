package org.wso2.carbon.inbound.iso8583.nio.listening;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.inbound.InboundProcessorParams;

public class ISO8583Session {
	private static final Log log = LogFactory.getLog(ISO8583Session.class);
	private final SocketChannel channel;
	private ExecutorService threadPool;
	private InboundProcessorParams params;
	private ArrayDeque<byte[]> messageQueue = new ArrayDeque<byte[]>();

	public ISO8583Session(SocketChannel channel, ExecutorService threadPool, InboundProcessorParams params) {
		super();
		this.channel = channel;
		this.threadPool = threadPool;
		this.params = params;
	}

	// read from the socket channel

	public void read() throws IOException {
		log.debug("Trying to read data from socket");
		ByteBuffer headerbuffer = ByteBuffer.allocate(4);
		int numRead = -1;
		numRead = channel.read(headerbuffer);
		int len = 0;
		if (numRead == 4) {
			byte[] header = new byte[numRead];
			System.arraycopy(headerbuffer.array(), 0, header, 0, 4);
			try {
				len = this.parseHeader(header);
			} catch (NumberFormatException e) {
				log.error("The lengh of the message was not a number");
				len = 100;
			}
		}
		ByteBuffer buffer = ByteBuffer.allocate(len);
		numRead = -1;
		numRead = channel.read(buffer);

		if (numRead > 0) {

			byte[] data = new byte[numRead];

			System.arraycopy(buffer.array(), 0, data, 0, numRead);
			if (log.isDebugEnabled()) {
				log.debug("Got: " + new String(data));
			}
			if (numRead == len) {
				handleClientRequest(data);
			} else {
				log.error("Data did not match header expected " + len + " bytes, got " + numRead);
			}

		} else {

			Socket socket = channel.socket();
			SocketAddress remoteAddr = socket.getRemoteSocketAddress();
			log.warn("Connection closed by client: " + remoteAddr);
			channel.close();

		}
	}

	public void write() throws IOException {
		if (!messageQueue.isEmpty()) {
			byte[] message = messageQueue.pop();
			sendResponse(message);
		}
	}

	private int parseHeader(byte[] header) {
		String hs = new String(header);
		return Integer.parseInt(hs.trim());

	}

	private void handleClientRequest(byte[] message) {
		try {
			threadPool.submit(new ISO8583MessageRequestHandler(message, this, params));

		} catch (RejectedExecutionException re) {

			// If the pool is full complete the execution with the same thread
			log.error("Worker pool has reached the maximum capacity.");
		}
	}

	public void send(byte[] msg) {
		messageQueue.add(msg);
	}

	/**
	 * writes the packed iso message response to the client.
	 *
	 * @param responseMessage
	 *            String of packed ISO response.
	 */
	private void sendResponse(byte[] msg) {
		if (log.isDebugEnabled()) {
			log.debug("Sending" + msg);
		}
		msg = makeHeader(msg);
		ByteBuffer buf = ByteBuffer.allocate(msg.length);
		buf.clear();
		buf.put(msg);

		buf.flip();
		try {
			while (buf.hasRemaining()) {
				log.debug(":");
				channel.write(buf);
			}
			log.debug("done");
		} catch (IOException e) {
			log.error("Error while sending responce ", e);
		}
	}

	private byte[] makeHeader(byte[] data) {
		int len = data.length;
		String s = String.format("%04d", len);
		// Integer.toString(len);
		if (log.isDebugEnabled()) {
			log.debug("Header: " + s);
		}
		byte[] header = s.getBytes();
		int hl = header.length;
		byte buf[] = new byte[4 + len];

		System.arraycopy(header, 0, buf, 0, hl);

		System.arraycopy(data, 0, buf, hl, len);

		return buf;
	}

}
