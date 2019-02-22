package org.wso2.carbon.inbound.iso8583.nio.listening;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AcceptPendingException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.inbound.InboundProcessorParams;
import org.wso2.carbon.inbound.iso8583.listening.ISO8583Constant;

public class ISO8583MessageAsyncConnection extends Thread {
	private static final Log log = LogFactory.getLog(ISO8583MessageAsyncConnection.class);
	private static final long READ_MESSAGE_WAIT_TIME = 0;
	private AsynchronousChannelGroup asyncChannelGroup;
	private AsynchronousServerSocketChannel asyncServerSocketChannel;
	private InetSocketAddress listenAddress;
	private ExecutorService threadPool;
	private InboundProcessorParams params;
	private boolean isRunning;

	public ISO8583MessageAsyncConnection(int port, InboundProcessorParams params) {
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
		} catch (NumberFormatException e) {
			handleException("One of the property or properties of thread specified is of an invalid type", e);
		}
		return threadPool;
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

		try {
			asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(threadPool);
			asyncServerSocketChannel = AsynchronousServerSocketChannel.open(asyncChannelGroup).bind(listenAddress);
			if (asyncServerSocketChannel.isOpen()) {
				// The accept method does not block it sets up the
				// CompletionHandler callback and moves on.
				asyncServerSocketChannel.accept(null, new CompletionHandler<AsynchronousSocketChannel, Object>() {
					@Override
					public void completed(final AsynchronousSocketChannel asyncSocketChannel, Object attachment) {
						if (asyncServerSocketChannel.isOpen()) {
							asyncServerSocketChannel.accept(null, this);
						}
						handleAcceptConnection(asyncSocketChannel);
					}

					@Override
					public void failed(Throwable exc, Object attachment) {
						if (asyncServerSocketChannel.isOpen()) {
							asyncServerSocketChannel.accept(null, this);
							System.out.println("***********" + exc + " statement=" + attachment);
						}
					}
				});
				log.info("Server " + getName() + " reading to accept first connection...");
			}
		} catch (AcceptPendingException ex) {
			ex.printStackTrace();
		}
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

	private void handleAcceptConnection(AsynchronousSocketChannel asyncSocketChannel) {
		log.info(">>handleAcceptConnection(), asyncSocketChannel=" + asyncSocketChannel);
		ByteBuffer messageByteBuffer = ByteBuffer.allocate(77);
		try {
			// read a message from the client, timeout after 10 seconds
			Future<Integer> futureReadResult = asyncSocketChannel.read(messageByteBuffer);
			futureReadResult.get(READ_MESSAGE_WAIT_TIME, TimeUnit.SECONDS);

			String clientMessage = new String(messageByteBuffer.array()).trim();

			messageByteBuffer.clear();
			messageByteBuffer.flip();

			String responseString = "echo" + "_" + clientMessage;
			messageByteBuffer = ByteBuffer.wrap((responseString.getBytes()));
			Future<Integer> futureWriteResult = asyncSocketChannel.write(messageByteBuffer);
			futureWriteResult.get(READ_MESSAGE_WAIT_TIME, TimeUnit.SECONDS);
			if (messageByteBuffer.hasRemaining()) {
				messageByteBuffer.compact();
			} else {
				messageByteBuffer.clear();
			}
		} catch (InterruptedException | ExecutionException | TimeoutException | CancellationException e) {
			log.error(e);
		} finally {
			try {
				asyncSocketChannel.close();
			} catch (IOException ioEx) {
				log.error(ioEx);
			}
		}
	}

	public void stopServer() throws IOException {
		log.info(">>stopingServer()...");
		this.asyncServerSocketChannel.close();
		this.asyncChannelGroup.shutdown();
	}

}
