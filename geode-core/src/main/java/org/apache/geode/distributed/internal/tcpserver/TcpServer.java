/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal.tcpserver;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.IncompatibleVersionException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolService;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceLoadingFailureException;
import org.apache.geode.internal.cache.client.protocol.exception.ServiceVersionNotFoundException;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;

/**
 * TCP server which listens on a port and delegates requests to a request handler. The server uses
 * expects messages containing a global version number, followed by a DataSerializable object
 * <p>
 * This code was factored out of GossipServer.java to allow multiple handlers to share the same
 * gossip server port.
 *
 * @since GemFire 5.7
 */
public class TcpServer {

  /**
   * The version of the tcp server protocol
   * <p>
   * This should be incremented if the gossip message structures change
   * <p>
   * 0 - special indicator of a non-gossip message from a client<br>
   * 1000 - gemfire 5.5 - using java serialization<br>
   * 1001 - 5.7 - using DataSerializable and supporting server locator messages.<br>
   * 1002 - 7.1 - sending GemFire version along with GOSSIP_VERSION in each request.
   * <p>
   * with the addition of support for all old versions of clients you can no longer change this
   * version number
   */
  public static final int GOSSIPVERSION = 1002;

  // Don't change it ever. We did NOT send GemFire version in a Gossip request till 1001 version.
  // This GOSSIPVERSION is used in _getVersionForAddress request for getting GemFire version of a
  // GossipServer.
  public static final int OLDGOSSIPVERSION = 1001;

  @MutableForTesting("The map used here is mutable, because some tests modify it")
  private static final Map<Integer, Short> GOSSIP_TO_GEMFIRE_VERSION_MAP =
      createGossipToVersionMap();

  // For test purpose only
  @MutableForTesting
  public static boolean isTesting = false;
  // Non-final field for testing to avoid any security holes in system.
  @MutableForTesting
  public static int TESTVERSION = GOSSIPVERSION;
  @MutableForTesting
  public static int OLDTESTVERSION = OLDGOSSIPVERSION;

  public static final long SHUTDOWN_WAIT_TIME = 60 * 1000;
  private static final int MAX_POOL_SIZE =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "TcpServer.MAX_POOL_SIZE", 100);
  private static final int POOL_IDLE_TIMEOUT = 60 * 1000;

  private static final Logger log = LogService.getLogger();

  // no longer static so that tests can test this system property
  private final int READ_TIMEOUT =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "TcpServer.READ_TIMEOUT", 60 * 1000);
  private static final int P2P_BACKLOG = Integer.getInteger("p2p.backlog", 1000);
  private static final int BACKLOG =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "TcpServer.BACKLOG", P2P_BACKLOG);

  private int port;
  private ServerSocket srv_sock = null;
  private InetAddress bind_address;
  private volatile boolean shuttingDown = false;
  private final PoolStatHelper poolHelper;
  private final InternalLocator internalLocator;
  private final TcpHandler handler;
  private final ClientProtocolServiceLoader clientProtocolServiceLoader;


  private ExecutorService executor;
  private final String threadName;
  private volatile Thread serverThread;

  protected SocketCreator socketCreator;

  /*
   * Initialize versions map. Warning: This map must be compatible with all GemFire versions being
   * handled by this member "With different GOSSIPVERION". If GOSSIPVERIONS are same for then
   * current GOSSIPVERSION should be used.
   */
  private static Map<Integer, Short> createGossipToVersionMap() {
    HashMap<Integer, Short> map = new HashMap<>();
    map.put(GOSSIPVERSION, Version.GFE_71.ordinal());
    map.put(OLDGOSSIPVERSION, Version.GFE_57.ordinal());
    return map;
  }

  public TcpServer(int port, InetAddress bind_address, Properties sslConfig,
      DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper,
      String threadName, InternalLocator internalLocator,
      ClientProtocolServiceLoader clientProtocolServiceLoader) {
    this.port = port;
    this.bind_address = bind_address;
    this.handler = handler;
    this.poolHelper = poolHelper;
    this.internalLocator = internalLocator;
    this.clientProtocolServiceLoader = clientProtocolServiceLoader;
    this.executor = createExecutor(poolHelper);
    this.threadName = threadName;

    if (cfg == null) {
      if (sslConfig == null) {
        sslConfig = new Properties();
      }
      cfg = new DistributionConfigImpl(sslConfig);
    }
  }

  protected SocketCreator getSocketCreator() {
    if (this.socketCreator == null) {
      this.socketCreator =
          SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR);
    }
    return socketCreator;
  }

  private static ExecutorService createExecutor(PoolStatHelper poolHelper) {
    return LoggingExecutors.newThreadPoolWithSynchronousFeed("locator request thread ",
        MAX_POOL_SIZE, poolHelper, POOL_IDLE_TIMEOUT, new ThreadPoolExecutor.CallerRunsPolicy());
  }

  public void restarting(InternalDistributedSystem ds, InternalCache cache,
      InternalConfigurationPersistenceService sharedConfig) throws IOException {
    this.shuttingDown = false;
    this.handler.restarting(ds, cache, sharedConfig);
    startServerThread();
    this.executor = createExecutor(this.poolHelper);
    log.info("TcpServer@" + System.identityHashCode(this)
        + " restarting: completed.  Server thread=" + this.serverThread + '@'
        + System.identityHashCode(this.serverThread) + ";alive=" + this.serverThread.isAlive());
  }

  public void restartCompleted(InternalDistributedSystem ds) {
    this.handler.restartCompleted(ds);
  }

  public void start() throws IOException {
    this.shuttingDown = false;
    startServerThread();
    handler.init(this);
  }

  private void startServerThread() throws IOException {
    initializeServerSocket();
    if (serverThread == null || !serverThread.isAlive()) {
      serverThread = new LoggingThread(threadName, this::run);
      serverThread.start();
    }
  }

  private void initializeServerSocket() throws IOException {
    if (srv_sock == null || srv_sock.isClosed()) {
      if (bind_address == null) {
        srv_sock = getSocketCreator().createServerSocket(port, BACKLOG);
        bind_address = srv_sock.getInetAddress();
      } else {
        srv_sock = getSocketCreator().createServerSocket(port, BACKLOG, bind_address);
      }
      // GEODE-4176 - set the port from a wild-card bind so that handlers know the correct value
      if (this.port <= 0) {
        this.port = srv_sock.getLocalPort();
      }

      if (log.isInfoEnabled()) {
        log.info("Locator was created at " + new Date());
        log.info("Listening on port " + getPort() + " bound on address " + bind_address);
      }
      srv_sock.setReuseAddress(true);
    }
  }

  public void join(long millis) throws InterruptedException {
    if (serverThread != null) {
      serverThread.join(millis);
    }
  }

  public void join() throws InterruptedException {
    if (serverThread != null) {
      serverThread.join();
    }
  }

  public boolean isAlive() {
    return serverThread != null && serverThread.isAlive();
  }

  public boolean isShuttingDown() {
    return this.shuttingDown;
  }

  public SocketAddress getBindAddress() {
    return srv_sock.getLocalSocketAddress();
  }

  /**
   * Returns the value of the bound port. If the server was initialized with a port of 0 indicating
   * that any ephemeral port should be used, this method will return the actual bound port.
   *
   * @return the locator's tcp/ip port. This will be zero if the TcpServer hasn't been started.
   */
  public int getPort() {
    return port;
  }

  protected void run() {
    Socket sock = null;

    while (!shuttingDown) {
      if (SystemFailure.getFailure() != null) {
        // Allocate no objects here!
        try {
          srv_sock.close();
          return;
        } catch (IOException ignore) {
          // ignore
        }
        SystemFailure.checkFailure(); // throws
      }
      if (srv_sock.isClosed()) {
        shuttingDown = true;
        break;
      }
      try {
        try {
          sock = srv_sock.accept();
        } catch (SSLException ex) {
          // SW: This is the case when there is a problem in locator
          // SSL configuration, so need to exit otherwise goes into an
          // infinite loop just filling the logs
          log.error("Locator stopping due to SSL configuration problem.", ex);
          shuttingDown = true;
          continue;
        }
        processRequest(sock);
      } catch (Exception ex) {
        if (!shuttingDown) {
          log.error("exception=", ex);
        }
        continue;
      }
    }

    if (!srv_sock.isClosed()) {
      try {
        srv_sock.close();
      } catch (java.io.IOException ex) {
        log.warn("exception closing server socket during shutdown", ex);
      }
    }

    if (shuttingDown) {
      log.info("locator shutting down");
      executor.shutdown();
      try {
        executor.awaitTermination(SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
      handler.shutDown();
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  /**
   * fix for bug 33711 - client requests are spun off to another thread for processing. Requests are
   * synchronized in processGossip.
   */
  private void processRequest(final Socket socket) {
    executor.execute(() -> {
      long startTime = DistributionStats.getStatTime();
      DataInputStream input = null;
      try {
        socket.setSoTimeout(READ_TIMEOUT);
        getSocketCreator().handshakeIfSocketIsSSL(socket, READ_TIMEOUT);

        try {
          input = new DataInputStream(socket.getInputStream());
        } catch (StreamCorruptedException e) {
          // Some garbage can be left on the socket stream
          // if a peer disappears at exactly the wrong moment.
          log.debug("Discarding illegal request from "
              + (socket.getInetAddress().getHostAddress() + ":" + socket.getPort()), e);
          return;
        }
        // read the first byte & check for an improperly configured client pool trying
        // to contact a cache server
        int firstByte = input.readUnsignedByte();
        if (firstByte == CommunicationMode.ReservedForGossip.getModeNumber()) {
          processOneConnection(socket, startTime, input);
        } else if (firstByte == CommunicationMode.ProtobufClientServerProtocol.getModeNumber()) {
          handleProtobufConnection(socket, input);
        } else if (CommunicationMode.isValidMode(firstByte)) {
          socket.getOutputStream().write(Handshake.REPLY_SERVER_IS_LOCATOR);
          throw new Exception("Improperly configured client detected - use addPoolLocator to "
              + "configure its locators instead of addPoolServer.");

        } else {
          rejectUnknownProtocolConnection(socket, firstByte);
        }
      } catch (EOFException | SocketException ignore) {
        // client went away - ignore
      } catch (CancelException ignore) {
        // ignore
      } catch (SocketTimeoutException ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        // Do not want the full stack trace to fill up the logs
        log.info("Exception in processing request from " + sender + ": " + ex.getMessage());
      } catch (ClassNotFoundException ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        log.info("Unable to process request from " + sender + " exception=" + ex.getMessage());
      } catch (Exception ex) {
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        if (ex instanceof IOException) {
          // IOException could be caused by a client failure. Don't
          // log with severe.
          if (!socket.isClosed()) {
            log.info("Exception in processing request from " + sender, ex);
          }
        } else {
          log.fatal("Exception in processing request from " + sender, ex);
        }

      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        throw err;
      } catch (Throwable ex) {
        SystemFailure.checkFailure();
        String sender = null;
        if (socket != null) {
          sender = socket.getInetAddress().getHostAddress();
        }
        try {
          log.fatal("Exception in processing request from " + sender, ex);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          throw err;
        } catch (Throwable t) {
          SystemFailure.checkFailure();
          t.printStackTrace();
        }
      } finally {
        try {
          socket.close();
        } catch (IOException ignore) {
          // ignore
        }
      }
    });
  }

  private void processOneConnection(Socket socket, long startTime, DataInputStream input)
      throws IOException, UnsupportedSerializationVersionException, ClassNotFoundException {
    // At this point we've read the leading byte of the gossip version and found it to be 0,
    // continue reading the next three bytes
    int gossipVersion = 0;
    for (int i = 0; i < 3; i++) {
      gossipVersion = (gossipVersion << 8) + (0xff & input.readUnsignedByte());
    }

    Object request;
    Object response;
    short versionOrdinal;
    if (gossipVersion <= getCurrentGossipVersion()
        && GOSSIP_TO_GEMFIRE_VERSION_MAP.containsKey(gossipVersion)) {
      // Create a versioned stream to remember sender's GemFire version
      versionOrdinal = (short) GOSSIP_TO_GEMFIRE_VERSION_MAP.get(gossipVersion);

      if (Version.GFE_71.compareTo(versionOrdinal) <= 0) {
        // Recent versions of TcpClient will send the version ordinal
        versionOrdinal = input.readShort();
      }

      if (log.isDebugEnabled() && versionOrdinal != Version.CURRENT_ORDINAL) {
        log.debug("Locator reading request from " + socket.getInetAddress() + " with version "
            + Version.fromOrdinal(versionOrdinal));
      }
      input = new VersionedDataInputStream(input, Version.fromOrdinal(versionOrdinal));
      request = DataSerializer.readObject(input);
      if (log.isDebugEnabled()) {
        log.debug("Locator received request " + request + " from " + socket.getInetAddress());
      }
      if (request instanceof ShutdownRequest) {
        shuttingDown = true;
        // Don't call shutdown from within the worker thread, see java bug #6576792.
        // Closing the socket will cause our acceptor thread to shutdown the executor
        srv_sock.close();
        response = new ShutdownResponse();
      } else if (request instanceof InfoRequest) {
        response = handleInfoRequest(request);
      } else if (request instanceof VersionRequest) {
        response = handleVersionRequest(request);
      } else {
        response = handler.processRequest(request);
      }

      handler.endRequest(request, startTime);

      startTime = DistributionStats.getStatTime();
      if (response != null) {
        DataOutputStream output = new DataOutputStream(socket.getOutputStream());
        if (versionOrdinal != Version.CURRENT_ORDINAL) {
          output =
              new VersionedDataOutputStream(output, Version.fromOrdinal(versionOrdinal));
        }
        DataSerializer.writeObject(response, output);
        output.flush();
      }

      handler.endResponse(request, startTime);
    } else {
      // Close the socket. We can not accept requests from a newer version
      rejectUnknownProtocolConnection(socket, gossipVersion);
    }
  }

  private void rejectUnknownProtocolConnection(Socket socket, int gossipVersion) {
    try {
      socket.getOutputStream().write("unknown protocol version".getBytes());
      socket.getOutputStream().flush();
      socket.close();
    } catch (IOException e) {
      log.debug("exception in sending reply to process using unknown protocol " + gossipVersion, e);
    }
  }

  private void handleProtobufConnection(Socket socket, DataInputStream input) throws Exception {
    if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
      log.warn("Incoming protobuf connection, but protobuf not enabled on this locator.");
      socket.close();
      return;
    }

    try {
      ClientProtocolService clientProtocolService = clientProtocolServiceLoader.lookupService();
      clientProtocolService.initializeStatistics("LocatorStats",
          internalLocator.getDistributedSystem());
      try (ClientProtocolProcessor pipeline = clientProtocolService.createProcessorForLocator(
          internalLocator, internalLocator.getCache().getSecurityService())) {
        while (!pipeline.socketProcessingIsFinished()) {
          pipeline.processMessage(input, socket.getOutputStream());
        }
      } catch (IncompatibleVersionException e) {
        // should not happen on the locator as there is no handshake.
        log.error("Unexpected exception in client message processing", e);
      }
    } catch (ServiceLoadingFailureException e) {
      log.error("There was an error looking up the client protocol service", e);
      socket.close();
      throw new IOException("There was an error looking up the client protocol service", e);
    } catch (ServiceVersionNotFoundException e) {
      log.error("Unable to find service matching the client protocol version byte", e);
      socket.close();
      throw new IOException("Unable to find service matching the client protocol version byte", e);
    }
  }

  protected Object handleInfoRequest(Object request) {
    String[] info = new String[2];
    info[0] = System.getProperty("user.dir");

    URL url = GemFireVersion.getJarURL();
    if (url == null) {
      String s = "Could not find gemfire jar";
      throw new IllegalStateException(s);
    }

    File gemfireJar = new File(url.getPath());
    File lib = gemfireJar.getParentFile();
    File product = lib.getParentFile();
    info[1] = product.getAbsolutePath();

    return new InfoResponse(info);
  }

  protected Object handleVersionRequest(Object request) {
    VersionResponse response = new VersionResponse();
    response.setVersionOrdinal(Version.CURRENT_ORDINAL);
    return response;
  }

  public static int getCurrentGossipVersion() {
    return TcpServer.isTesting ? TcpServer.TESTVERSION : TcpServer.GOSSIPVERSION;
  }

  public static int getOldGossipVersion() {
    return TcpServer.isTesting ? TcpServer.OLDTESTVERSION : TcpServer.OLDGOSSIPVERSION;
  }

  public static Map<Integer, Short> getGossipVersionMapForTestOnly() {
    return GOSSIP_TO_GEMFIRE_VERSION_MAP;
  }

}
