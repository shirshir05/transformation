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

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getElectionAlg;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getInitLimit;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getServerId;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getServers;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getSyncLimit;
import static org.apache.zookeeper.server.quorum.QuorumPeerConfig.getTickTime;

/**
 * This class manages the quorum protocol. There are three states this server
 * can be in:
 * <ol>
 * <li>Leader election - each server will elect a leader (proposing itself as a
 * leader initially).</li>
 * <li>Follower - the server will synchronize with the leader and replicate any
 * transactions.</li>
 * <li>Leader - the server will process requests and forward them to followers.
 * A majority of followers must log the request before it can be accepted.
 * </ol>
 *
 * This class will setup a datagram socket that will always respond with its
 * view of the current leader. The response will take the form of:
 *
 * <pre>
 * int xid;
 *
 * long myid;
 *
 * long leader_id;
 *
 * long leader_zxid;
 * </pre>
 *
 * The request for the current leader will consist solely of an xid: int xid;
 */
public class QuorumPeer extends Thread implements QuorumStats.Provider {

    private static final Logger LOG = Logger.getLogger(QuorumPeer.class);

    /**
     * Create an instance of a quorum peer
     */
    public interface Factory {

        public QuorumPeer create(NIOServerCnxn.Factory cnxnFactory) throws IOException;

        public NIOServerCnxn.Factory createConnectionFactory() throws IOException;
    }

    public static class QuorumServer {

        public QuorumServer(long id, InetSocketAddress addr, InetSocketAddress electionAddr) {
            this.id = id;
            this.addr = addr;
            this.electionAddr = electionAddr;
        }

        public QuorumServer(long id, InetSocketAddress addr) {
            this.id = id;
            this.addr = addr;
        }

        public InetSocketAddress addr;

        public InetSocketAddress electionAddr;

        public long id;
    }

    public enum ServerState {

        LOOKING, FOLLOWING, LEADING
    }

    /**
     * The servers that make up the cluster
     */
    HashMap<Long, QuorumServer> quorumPeers;

    public int getQuorumSize() {
        return quorumPeers.size();
    }

    /**
     * My id
     */
    private long myid;

    /**
     * get the id of this quorum peer.
     */
    public long getId() {
        return myid;
    }

    /**
     * This is who I think the leader currently is.
     */
    volatile private Vote currentVote;

    public synchronized Vote getCurrentVote() {
        return currentVote;
    }

    public synchronized void setCurrentVote(Vote v) {
        currentVote = v;
    }

    volatile boolean running = true;

    /**
     * The number of milliseconds of each tick
     */
    int tickTime;

    /**
     * The number of ticks that the initial synchronization phase can take
     */
    int initLimit;

    /**
     * The number of ticks that can pass between sending a request and getting
     * an acknowledgement
     */
    int syncLimit;

    /**
     * The current tick
     */
    int tick;

    /**
     * This class simply responds to requests for the current leader of this
     * node.
     * <p>
     * The request contains just an xid generated by the requestor.
     * <p>
     * The response has the xid, the id of this server, the id of the leader,
     * and the zxid of the leader.
     */
    class ResponderThread extends Thread {

        ResponderThread() {
            super("ResponderThread");
        }

        volatile boolean running = true;

        @Override
        public void run() {
            try {
                byte[] b = new byte[36];
                ByteBuffer responseBuffer = ByteBuffer.wrap(b);
                DatagramPacket packet = new DatagramPacket(b, b.length);
                while (running) {
                    udpSocket.receive(packet);
                    if (packet.getLength() != 4) {
                        LOG.warn("Got more than just an xid! Len = " + packet.getLength());
                    } else {
                        responseBuffer.clear();
                        // Skip the xid
                        responseBuffer.getInt();
                        responseBuffer.putLong(myid);
                        Vote current = getCurrentVote();
                        switch(getPeerState()) {
                            case LOOKING:
                                responseBuffer.putLong(current.id);
                                responseBuffer.putLong(current.zxid);
                                break;
                            case LEADING:
                                responseBuffer.putLong(myid);
                                try {
                                    responseBuffer.putLong(leader.lastProposed);
                                } catch (NullPointerException npe) {
                                }
                                break;
                            case FOLLOWING:
                                responseBuffer.putLong(current.id);
                                try {
                                    responseBuffer.putLong(follower.getZxid());
                                } catch (NullPointerException npe) {
                                }
                        }
                        packet.setData(b);
                        udpSocket.send(packet);
                    }
                    packet.setLength(b.length);
                }
            } catch (Exception e) {
                LOG.warn("Unexpected exception", e);
            } finally {
                LOG.warn("QuorumPeer responder thread exited");
            }
        }
    }

    private ServerState state = ServerState.LOOKING;

    public synchronized void setPeerState(ServerState newState) {
        state = newState;
    }

    public synchronized ServerState getPeerState() {
        return state;
    }

    DatagramSocket udpSocket;

    private InetSocketAddress myQuorumAddr;

    public InetSocketAddress getQuorumAddress() {
        return myQuorumAddr;
    }

    private int electionType;

    Election electionAlg;

    NIOServerCnxn.Factory cnxnFactory;

    private FileTxnSnapLog logFactory = null;

    public QuorumPeer() {
        super("QuorumPeer");
        QuorumStats.getInstance().setStatsProvider(this);
    }

    public QuorumPeer(HashMap<Long, QuorumServer> quorumPeers, File dataDir, File dataLogDir, int electionType, long myid, int tickTime, int initLimit, int syncLimit, NIOServerCnxn.Factory cnxnFactory) throws IOException {
        super("QuorumPeer");
        this.cnxnFactory = cnxnFactory;
        this.quorumPeers = quorumPeers;
        this.electionType = electionType;
        this.myid = myid;
        this.tickTime = tickTime;
        this.initLimit = initLimit;
        this.syncLimit = syncLimit;
        this.logFactory = new FileTxnSnapLog(dataLogDir, dataDir);
        QuorumStats.getInstance().setStatsProvider(this);
    }

    @Override
    public synchronized void start() {
        startLeaderElection();
        super.start();
    }

    ResponderThread responder;

    public void stopLeaderElection() {
        responder.running = false;
        responder.interrupt();
    }

    synchronized public void startLeaderElection() {
        currentVote = new Vote(myid, getLastLoggedZxid());
        for (QuorumServer p : quorumPeers.values()) {
            if (p.id == myid) {
                myQuorumAddr = p.addr;
                break;
            }
        }
        if (myQuorumAddr == null) {
            throw new RuntimeException("My id " + myid + " not in the peer list");
        }
        if (electionType == 0) {
            try {
                udpSocket = new DatagramSocket(myQuorumAddr.getPort());
                responder = new ResponderThread();
                responder.start();
            } catch (SocketException e) {
                throw new RuntimeException(e);
            }
        }
        this.electionAlg = createElectionAlgorithm(electionType);
    }

    /**
     * This constructor is only used by the existing unit test code.
     * It defaults to FileLogProvider persistence provider.
     */
    public QuorumPeer(HashMap<Long, QuorumServer> quorumPeers, File snapDir, File logDir, int clientPort, int electionAlg, long myid, int tickTime, int initLimit, int syncLimit) throws IOException {
        this(quorumPeers, snapDir, logDir, electionAlg, myid, tickTime, initLimit, syncLimit, new NIOServerCnxn.Factory(clientPort));
    }

    public long getLastLoggedZxid() {
        return logFactory.getLastLoggedZxid();
    }

    public Follower follower;

    public Leader leader;

    protected Follower makeFollower(FileTxnSnapLog logFactory) throws IOException {
        return new Follower(this, new FollowerZooKeeperServer(logFactory, this, new ZooKeeperServer.BasicDataTreeBuilder()));
    }

    protected Leader makeLeader(FileTxnSnapLog logFactory) throws IOException {
        return new Leader(this, new LeaderZooKeeperServer(logFactory, this, new ZooKeeperServer.BasicDataTreeBuilder()));
    }

    private Election createElectionAlgorithm(int electionAlgorithm) {
        Election le = null;
        // TODO: use a factory rather than a switch
        switch(electionAlgorithm) {
            case 0:
                // will create a new instance for each run of the protocol
                break;
            case 1:
                le = new AuthFastLeaderElection(this);
                break;
            case 2:
                le = new AuthFastLeaderElection(this, true);
                break;
            case 3:
                le = new FastLeaderElection(this, new QuorumCnxManager(this));
            default:
                assert false;
        }
        return le;
    }

    protected Election makeLEStrategy() {
        if (electionAlg == null)
            return new LeaderElection(this);
        return electionAlg;
    }

    synchronized protected void setLeader(Leader newLeader) {
        leader = newLeader;
    }

    synchronized protected void setFollower(Follower newFollower) {
        follower = newFollower;
    }

    synchronized public ZooKeeperServer getActiveServer() {
        if (leader != null)
            return leader.zk;
        else if (follower != null)
            return follower.zk;
        return null;
    }

    @Override
    public void run() {
        /*
         * Main loop
         */
        while (running) {
            switch(getPeerState()) {
                case LOOKING:
                    try {
                        LOG.info("LOOKING");
                        setCurrentVote(makeLEStrategy().lookForLeader());
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
                case FOLLOWING:
                    try {
                        LOG.info("FOLLOWING");
                        setFollower(makeFollower(logFactory));
                        follower.followLeader();
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                    } finally {
                        follower.shutdown();
                        setFollower(null);
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
                case LEADING:
                    LOG.info("LEADING");
                    try {
                        setLeader(makeLeader(logFactory));
                        leader.lead();
                        setLeader(null);
                    } catch (Exception e) {
                        LOG.warn("Unexpected exception", e);
                    } finally {
                        if (leader != null) {
                            leader.shutdown("Forcing shutdown");
                            setLeader(null);
                        }
                        setPeerState(ServerState.LOOKING);
                    }
                    break;
            }
        }
        LOG.warn("QuorumPeer main thread exited");
    }

    public void shutdown() {
        running = false;
        if (leader != null) {
            leader.shutdown("quorum Peer shutdown");
        }
        if (follower != null) {
            follower.shutdown();
        }
        cnxnFactory.shutdown();
        udpSocket.close();
    }

    public String[] getQuorumPeers() {
        List<String> l = new ArrayList<String>();
        synchronized (this) {
            if (leader != null) {
                synchronized (leader.followers) {
                    for (FollowerHandler fh : leader.followers) {
                        if (fh.sock == null)
                            continue;
                        String s = fh.sock.getRemoteSocketAddress().toString();
                        if (leader.isFollowerSynced(fh))
                            s += "*";
                        l.add(s);
                    }
                }
            } else if (follower != null) {
                l.add(follower.sock.getRemoteSocketAddress().toString());
            }
        }
        return l.toArray(new String[0]);
    }

    public String getServerState() {
        switch(getPeerState()) {
            case LOOKING:
                return QuorumStats.Provider.LOOKING_STATE;
            case LEADING:
                return QuorumStats.Provider.LEADING_STATE;
            case FOLLOWING:
                return QuorumStats.Provider.FOLLOWING_STATE;
        }
        return QuorumStats.Provider.UNKNOWN_STATE;
    }

    /**
     * get the id of this quorum peer.
     */
    public long getMyid() {
        return myid;
    }

    /**
     * set the id of this quorum peer.
     */
    public void setMyid(long myid) {
        this.myid = myid;
    }

    /**
     * Get the number of milliseconds of each tick
     */
    public int getTickTime() {
        return tickTime;
    }

    /**
     * Set the number of milliseconds of each tick
     */
    public void setTickTime(int tickTime) {
        this.tickTime = tickTime;
    }

    /**
     * Get the number of ticks that the initial synchronization phase can take
     */
    public int getInitLimit() {
        return initLimit;
    }

    /**
     * Set the number of ticks that the initial synchronization phase can take
     */
    public void setInitLimit(int initLimit) {
        this.initLimit = initLimit;
    }

    public Election getElectionAlg() {
        return electionAlg;
    }

    /**
     * Get the number of ticks that can pass between sending a request and getting
     * an acknowledgement
     */
    public int getSyncLimit() {
        return syncLimit;
    }

    /**
     * Set the number of ticks that can pass between sending a request and getting
     * an acknowledgement
     */
    public void setSyncLimit(int syncLimit) {
        this.syncLimit = syncLimit;
    }

    /**
     * Gets the election type
     */
    public int getElectionType() {
        return electionType;
    }

    /**
     * Sets the election type
     */
    public void setElectionType(int electionType) {
        this.electionType = electionType;
    }

    public NIOServerCnxn.Factory getCnxnFactory() {
        return cnxnFactory;
    }

    public void setCnxnFactory(NIOServerCnxn.Factory cnxnFactory) {
        this.cnxnFactory = cnxnFactory;
    }

    public void setQuorumPeers(HashMap<Long, QuorumServer> var0) {
        this.var0 = var0;
    }

    public int getClientPort() {
        return -1;
    }

    public void setClientPort(int clientPort) {
    }

    public void setTxnFactory(FileTxnSnapLog factory) {
        this.logFactory = factory;
    }

    public FileTxnSnapLog getTxnFactory() {
        return this.logFactory;
    }
}
