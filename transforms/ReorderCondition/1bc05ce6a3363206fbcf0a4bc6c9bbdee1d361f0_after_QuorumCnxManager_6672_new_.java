/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class QuorumCnxManager {

    private static final Logger LOG = Logger.getLogger(QuorumCnxManager.class);

    static final int CAPACITY = 100;

    static final int MAX_CONNECTION_ATTEMPTS = 2;

    /*
     * Local IP address
     */
    QuorumPeer self;

    /*
     * Mapping from Peer to Thread number
     */
    ConcurrentHashMap<Long, SendWorker> senderWorkerMap;

    ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>> queueSendMap;

    ConcurrentHashMap<Long, ByteBuffer> lastMessageSent;

    /*
     * Reception queue
     */
    public ArrayBlockingQueue<Message> recvQueue;

    boolean shutdown = false;

    /*
     * Listener thread
     */
    public Listener listener;

    static class Message {

        Message(ByteBuffer buffer, long sid) {
            this.buffer = buffer;
            this.sid = sid;
        }

        ByteBuffer buffer;

        long sid;
    }

    public QuorumCnxManager(QuorumPeer self) {
        this.recvQueue = new ArrayBlockingQueue<Message>(CAPACITY);
        this.queueSendMap = new ConcurrentHashMap<Long, ArrayBlockingQueue<ByteBuffer>>();
        this.senderWorkerMap = new ConcurrentHashMap<Long, SendWorker>();
        this.lastMessageSent = new ConcurrentHashMap<Long, ByteBuffer>();
        this.self = self;
        // Starts listener thread that waits for connection requests
        listener = new Listener();
    }

    /**
     * Invokes initiateConnection for testing purposes
     *
     * @param sid
     */
    public void testInitiateConnection(long sid) throws Exception {
        SocketChannel channel;
        LOG.debug(sid + "Opening channel to server ");
        channel = SocketChannel.open(self.quorumPeers.get(sid).electionAddr);
        channel.socket().setTcpNoDelay(true);
        initiateConnection(channel, sid);
    }

    public boolean initiateConnection(SocketChannel s, Long sid) {
        try {
            // Sending id and challenge
            byte[] msgBytes = new byte[8];
            ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
            msgBuffer.putLong(self.getId());
            msgBuffer.position(0);
            s.write(msgBuffer);
        } catch (IOException e) {
            LOG.warn("Exception reading or writing challenge: ", e);
            return false;
        }
        // If lost the challenge, then drop the new connection
        if (sid > self.getId()) {
            try {
                LOG.info("Have smaller server identifier, so dropping the connection: (" + sid + ", " + self.getId() + ")");
                s.socket().close();
            } catch (IOException e) {
                LOG.warn("Ignoring exception when closing socket or trying to " + "reopen connection: ", e);
            }
        } else {
            SendWorker sw = new SendWorker(s, sid);
            RecvWorker rw = new RecvWorker(s, sid);
            sw.setRecv(rw);
            SendWorker vsw = senderWorkerMap.get(sid);
            senderWorkerMap.put(sid, sw);
            if (vsw != null)
                vsw.finish();
            if (!queueSendMap.containsKey(sid)) {
                queueSendMap.put(sid, new ArrayBlockingQueue<ByteBuffer>(CAPACITY));
            }
            sw.start();
            rw.start();
            return true;
        }
        return false;
    }

    /**
     * If this server receives a connection request, then it gives up on the new
     * connection if it wins. Notice that it checks whether it has a connection
     * to this server already or not. If it does, then it sends the smallest
     * possible long value to lose the challenge.
     */
    boolean receiveConnection(SocketChannel s) {
        Long sid = null;
        try {
            // Sending challenge and sid
            byte[] msgBytes = new byte[8];
            ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
            s.read(msgBuffer);
            msgBuffer.position(0);
            // Read server id
            sid = Long.valueOf(msgBuffer.getLong());
        } catch (IOException e) {
            LOG.info("Exception reading or writing challenge: " + e.toString());
            return false;
        }
        // If wins the challenge, then close the new connection.
        if (sid < self.getId()) {
            try {
                /*
                 * This replica might still believe that the connection to sid
                 * is up, so we have to shut down the workers before trying to
                 * open a new connection.
                 */
                SendWorker sw = senderWorkerMap.get(sid);
                if (sw != null)
                    sw.finish();
                /*
                 * Now we start a new connection
                 */
                LOG.debug("Create new connection to server: " + sid);
                s.socket().close();
                connectOne(sid);
            } catch (IOException e) {
                LOG.info("Error when closing socket or trying to reopen connection: " + e.toString());
            }
        } else {
            SendWorker sw = new SendWorker(s, sid);
            RecvWorker rw = new RecvWorker(s, sid);
            sw.setRecv(rw);
            SendWorker vsw = senderWorkerMap.get(sid);
            senderWorkerMap.put(sid, sw);
            if (vsw != null)
                vsw.finish();
            if (!queueSendMap.containsKey(sid)) {
                queueSendMap.put(sid, new ArrayBlockingQueue<ByteBuffer>(CAPACITY));
            }
            sw.start();
            rw.start();
            return true;
        }
        return false;
    }

    /**
     * Processes invoke this message to queue a message to send. Currently,
     * only leader election uses it.
     */
    public void toSend(Long sid, ByteBuffer b) {
        /*
         * If sending message to myself, then simply enqueue it (loopback).
         */
        if (self.getId() == sid) {
            try {
                b.position(0);
                recvQueue.put(new Message(b.duplicate(), sid));
            } catch (InterruptedException e) {
                LOG.warn("Exception when loopbacking", e);
            }
        } else
            try {
                /*
                 * Start a new connection if doesn't have one already.
                 */
                if (!queueSendMap.containsKey(sid)) {
                    ArrayBlockingQueue<ByteBuffer> bq = new ArrayBlockingQueue<ByteBuffer>(CAPACITY);
                    queueSendMap.put(sid, bq);
                    bq.put(b);
                } else {
                    ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                    if (bq != null) {
                        if (bq.remainingCapacity() == 0) {
                            bq.take();
                        }
                        bq.put(b);
                    } else {
                        LOG.error("No queue for server " + sid);
                    }
                }
                connectOne(sid);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while waiting to put message in queue.", e);
            }
    }

    synchronized void connectOne(long sid) {
        if ((senderWorkerMap.get(sid) == null)) {
            try {
                SocketChannel channel;
                LOG.debug("Opening channel to server " + sid);
                channel = SocketChannel.open(self.quorumPeers.get(sid).electionAddr);
                channel.socket().setTcpNoDelay(true);
                initiateConnection(channel, sid);
            } catch (IOException e) {
                LOG.warn("Cannot open channel to " + sid, e);
            }
        } else {
            LOG.error("There is a connection for server " + sid);
        }
    }

    void connectAll() {
        long sid;
        for (Enumeration<Long> en = queueSendMap.keys(); en.hasMoreElements(); ) {
            sid = en.nextElement();
            connectOne(sid);
        }
    }

    /**
     * Check if all queues are empty, indicating that all messages have been delivered.
     */
    boolean haveDelivered() {
        for (ArrayBlockingQueue<ByteBuffer> queue : queueSendMap.values()) {
            LOG.debug("Queue size: " + queue.size());
            if (queue.size() == 0)
                return true;
        }
        return false;
    }

    /**
     * Flag that it is time to wrap up all activities and interrupt the listener.
     */
    public void halt() {
        shutdown = true;
        LOG.debug("Halting listener");
        listener.halt();
        softHalt();
    }

    /**
     * A soft halt simply finishes workers.
     */
    public void softHalt() {
        for (SendWorker sw : senderWorkerMap.values()) {
            LOG.debug("Halting sender: " + sw);
            sw.finish();
        }
    }

    /**
     * Thread to listen on some port
     */
    public class Listener extends Thread {

        volatile ServerSocketChannel ss = null;

        /**
         * Sleeps on accept().
         */
        @Override
        public void run() {
            try {
                ss = ServerSocketChannel.open();
                int port = self.quorumPeers.get(self.getId()).electionAddr.getPort();
                LOG.info("My election bind port: " + port);
                ss.socket().setReuseAddress(true);
                ss.socket().bind(new InetSocketAddress(port));
                while (!shutdown) {
                    SocketChannel client = ss.accept();
                    Socket sock = client.socket();
                    sock.setTcpNoDelay(true);
                    LOG.debug("Connection request " + sock.getRemoteSocketAddress());
                    LOG.debug("Connection request: " + self.getId());
                    receiveConnection(client);
                }
            } catch (IOException e) {
                LOG.error("Listener.run: " + e.getMessage());
            }
        }

        /**
         * Halts this listener thread.
         */
        void halt() {
            try {
                LOG.debug("Trying to close listener: " + ss);
                if (ss != null) {
                    LOG.debug("Closing listener: " + self.getId());
                    ss.close();
                }
            } catch (IOException e) {
                LOG.warn("Exception when shutting down listener: " + e);
            }
        }
    }

    /**
     * Thread to send messages. Instance waits on a queue, and send a message as
     * soon as there is one available. If connection breaks, then opens a new
     * one.
     */
    class SendWorker extends Thread {

        Long sid;

        SocketChannel channel;

        RecvWorker recvWorker;

        volatile boolean running = true;

        /**
         * An instance of this thread receives messages to send
         * through a queue and sends them to the server sid.
         *
         * @param channel SocketChannel
         * @param sid   Server identifier
         */
        SendWorker(SocketChannel channel, Long sid) {
            this.sid = sid;
            this.channel = channel;
            recvWorker = null;
            LOG.debug("Address of remote peer: " + this.sid);
        }

        synchronized void setRecv(RecvWorker recvWorker) {
            this.recvWorker = recvWorker;
        }

        /**
         * Returns RecvWorker that pairs up with this SendWorker.
         *
         * @return RecvWorker
         */
        synchronized RecvWorker getRecvWorker() {
            return recvWorker;
        }

        synchronized boolean finish() {
            running = false;
            LOG.debug("Calling finish");
            this.interrupt();
            try {
                channel.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing socket");
            }
            this.interrupt();
            if (recvWorker != null)
                recvWorker.finish();
            senderWorkerMap.remove(sid);
            return running;
        }

        synchronized void send(ByteBuffer b) throws IOException {
            byte[] msgBytes = new byte[b.capacity() + (Integer.SIZE / 8)];
            ByteBuffer msgBuffer = ByteBuffer.wrap(msgBytes);
            msgBuffer.putInt(b.capacity());
            msgBuffer.put(b.array(), 0, b.capacity());
            msgBuffer.position(0);
            if (channel != null)
                channel.write(msgBuffer);
            else
                throw new IOException("SocketChannel is null");
        }

        @Override
        public void run() {
            try {
                ByteBuffer b = lastMessageSent.get(sid);
                if (b != null)
                    send(b);
            } catch (IOException e) {
                LOG.error("Failed to send last message. Shutting down thread.");
                this.finish();
            }
            while (running && !shutdown && channel != null) {
                ByteBuffer b = null;
                try {
                    ArrayBlockingQueue<ByteBuffer> bq = queueSendMap.get(sid);
                    if (bq != null)
                        b = bq.take();
                    else {
                        LOG.error("No queue of incoming messages for server " + sid);
                        this.finish();
                    }
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while waiting for message on queue", e);
                    continue;
                }
                if (b != null)
                    lastMessageSent.put(sid, b);
                try {
                    if (b != null)
                        send(b);
                } catch (Exception e) {
                    LOG.warn("Exception when using channel: " + sid, e);
                    this.finish();
                }
            }
            LOG.warn("Send worker leaving thread");
        }
    }

    /**
     * Thread to receive messages. Instance waits on a socket read. If the
     * channel breaks, then removes itself from the pool of receivers.
     */
    class RecvWorker extends Thread {

        Long sid;

        SocketChannel channel;

        volatile boolean running = true;

        RecvWorker(SocketChannel channel, Long sid) {
            this.sid = sid;
            this.channel = channel;
        }

        /**
         * Shuts down this worker
         *
         * @return boolean  Value of variable running
         */
        synchronized boolean finish() {
            running = false;
            this.interrupt();
            return running;
        }

        @Override
        public void run() {
            try {
                byte[] size = new byte[4];
                ByteBuffer msgLength = ByteBuffer.wrap(size);
                while (running && !shutdown && channel != null) {
                    /**
                     * Reads the first int to determine the length of the
                     * message
                     */
                    while (msgLength.hasRemaining()) {
                        if (channel.read(msgLength) < 0) {
                            throw new IOException("Channel eof");
                        }
                    }
                    msgLength.position(0);
                    int length = msgLength.getInt();
                    /**
                     * Allocates a new ByteBuffer to receive the message
                     */
                    if (length > 0) {
                        byte[] msgArray = new byte[length];
                        ByteBuffer message = ByteBuffer.wrap(msgArray);
                        int numbytes = 0;
                        while (message.hasRemaining()) {
                            numbytes += channel.read(message);
                        }
                        message.position(0);
                        synchronized (recvQueue) {
                            recvQueue.put(new Message(message.duplicate(), sid));
                        }
                        msgLength.position(0);
                    }
                }
            } catch (IOException e) {
                LOG.warn("Connection broken: ", e);
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while trying to add new " + "message to the reception queue", e);
            }
        }
    }
}
