package org.wso2.carbon.inbound.iso8583.nio.listening;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.inbound.InboundProcessorParams;
import org.wso2.carbon.inbound.iso8583.listening.ISO8583Constant;

public class ISO8583MessageConnection extends Thread {
	private static final Log log = LogFactory.getLog(ISO8583MessageConnection.class);
	private Selector selector;

	private InetSocketAddress listenAddress;
	private ExecutorService threadPool;
	private InboundProcessorParams params;
	private ServerSocketChannel serverChannel;
	private Set<String> whiteList;
	private boolean isRunning;

	public ISO8583MessageConnection(int port, InboundProcessorParams params) {
		log.debug("new ISO8583MessageConnection created");
		listenAddress = new InetSocketAddress(port);
		this.params = params;
		this.threadPool = getExecutorService();

	}

	/**
	 * create the threadPool to handle the concurrent request.
	 */
	private ExecutorService getExecutorService() {
		Properties properties = params.getProperties();
		String coreThreads = properties.getProperty(ISO8583Constant.INBOUND_CORE_THREADS);
		String maxThreads = properties.getProperty(ISO8583Constant.INBOUND_MAX_THREADS);
		String threadSafeTime = properties.getProperty(ISO8583Constant.INBOUND_THREAD_ALIVE);
		String queueLength = properties.getProperty(ISO8583Constant.INBOUND_THREAD_QLEN);
		String whiteListString = properties.getProperty(ISO8583Constant.INBOUND_WHITE_LIST);
		try {
			if ((!StringUtils.isEmpty(coreThreads)) && (!StringUtils.isEmpty(maxThreads))
					&& (!StringUtils.isEmpty(threadSafeTime)) && (!StringUtils.isEmpty(queueLength))) {
				BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(Integer.parseInt(queueLength));
				threadPool = new ThreadPoolExecutor(Integer.parseInt(coreThreads), Integer.parseInt(maxThreads),
						Integer.parseInt(threadSafeTime), TimeUnit.SECONDS, workQueue);
			} else {
				BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(
						Integer.parseInt(ISO8583Constant.THREAD_QLEN));
				threadPool = new ThreadPoolExecutor(Integer.parseInt(ISO8583Constant.CORE_THREADS),
						Integer.parseInt(ISO8583Constant.MAX_THREADS), Integer.parseInt(ISO8583Constant.KEEP_ALIVE),
						TimeUnit.SECONDS, workQueue);
			}
			if (!StringUtils.isEmpty(whiteListString)) {
				setWhiteList(whiteListString);
			}
		} catch (NumberFormatException e) {
			handleException("One of the property or properties of thread specified is of an invalid type", e);
		}
		return threadPool;
	}

	private void setWhiteList(String whiteListString) {
		StringTokenizer st = new StringTokenizer(whiteListString, ";");
		while (st.hasMoreTokens()) {
			String host = st.nextToken();
			addToWhiteList(host);
		}

	}

	private void addToWhiteList(String host) {
		if (whiteList == null) {
			whiteList = new HashSet<String>();
		}
		whiteList.add(host);
	}

	private void handleException(String string, NumberFormatException e) {
		log.error(string, e);

	}

	public void run() {
		this.isRunning = true;
		while (isRunning) {
			try {
				startServer();
			} catch (IOException e) {
				log.error("Server IO Exception ", e);
			}
		}
	}
	// create server channel

	private void startServer() throws IOException {
		log.info("Strating server");

		this.selector = Selector.open();

		serverChannel = ServerSocketChannel.open();
		serverChannel.configureBlocking(false);

		// retrieve server socket and bind to port
		serverChannel.socket().bind(listenAddress);
		serverChannel.register(this.selector, SelectionKey.OP_ACCEPT);

		log.info("ISO8583 Server started..." + listenAddress.getPort());
		while (isRunning && this.selector.isOpen()) {
			// wait for events
			log.trace("Waiting for event...");
			this.selector.select();
			log.trace("Procces event...");
			// work on selected keys
			Iterator<SelectionKey> keys = this.selector.selectedKeys().iterator();

			while (keys.hasNext() && isRunning) {

				SelectionKey key = (SelectionKey) keys.next();

				// this is necessary to prevent the same key from coming up
				// again the next time around.
				keys.remove();
				if (!key.isValid()) {
					continue;
				}
				if (key.isAcceptable()) {
					this.accept(key);
				} else if (key.isReadable()) {
					ISO8583Session session = getSession(key);
					session.read();
				} else if (key.isWritable()) {
					ISO8583Session session = getSession(key);
					session.write();
				}
			}
		}

		log.warn("Server stopped...");
	}

	public ISO8583Session getSession(SelectionKey key) {
		ISO8583Session session = (ISO8583Session) key.attachment();
		if (session == null) {

			SocketChannel channel = (SocketChannel) key.channel();
			session = new ISO8583Session(channel, threadPool, params);
			key.attach(session);
		}
		return session;
	}

	// accept a connection made to this channel's socket
	private void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
		SocketChannel channel = serverChannel.accept();
		channel.configureBlocking(false);
		Socket socket = channel.socket();
		SocketAddress remoteAddr = socket.getRemoteSocketAddress();
		log.info("Connection made from: " + remoteAddr);
		if (checkWhitelist(remoteAddr)) {
			channel.register(this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		}else{
			channel.close();
			log.warn("Closed connection made from unknow host: " + remoteAddr);
		}
	}

	private boolean checkWhitelist(SocketAddress remoteAddr) {
		if (whiteList == null) {
			return true;
		}
		String host = remoteAddr.toString();
		int ipEndIndex = host.indexOf(":");
		String hostIp = host.substring(1, ipEndIndex);
		log.info("Checking white list for: " + hostIp);
		if (whiteList.contains(hostIp)) {
			return true;
		}
		return false;
	}

	public void destroyConnection() {

		this.isRunning = false;

		if (this.serverChannel != null && this.serverChannel.isOpen()) {

			try {

				this.serverChannel.close();

			} catch (IOException e) {

				log.error("Exception while closing server socket");
			}
		}

		try {

			Iterator<SelectionKey> keys = this.selector.keys().iterator();

			while (keys.hasNext()) {

				SelectionKey key = keys.next();

				SelectableChannel channel = key.channel();

				if (channel instanceof SocketChannel) {

					SocketChannel socketChannel = (SocketChannel) channel;
					Socket socket = socketChannel.socket();
					String remoteHost = socket.getRemoteSocketAddress().toString();

					log.info("closing socket {}" + remoteHost);

					try {

						socketChannel.close();

					} catch (IOException e) {

						log.warn("Exception while closing socket", e);
					}

					key.cancel();
				}
			}

			log.info("closing selector");
			selector.close();

		} catch (Exception ex) {

			log.error("Exception while closing selector", ex);
		}
		this.interrupt();
	}

}
