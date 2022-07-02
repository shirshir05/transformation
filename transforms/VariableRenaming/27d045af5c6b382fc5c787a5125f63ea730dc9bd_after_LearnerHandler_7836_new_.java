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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
public class LearnerHandler extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    final Leader leader;

    long tickOfLastAck;

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    protected int version = 0x1;

    int getVersion() {
        return version;
    }

    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private BufferedOutputStream bufferedOutput;

    LearnerHandler(Socket sock, Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        leader.addLearnerHandler(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfLastAck:").append(tickOfLastAck());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType learnerType = LearnerType.PARTICIPANT;

    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // the learner/observer instantaneously
                        sock.close();
                    } catch (IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        if (true)
            return null;
        String type = null;
        String mess = null;
        Record txn = null;
        switch(p.getType()) {
            case Leader.ACK:
                type = "ACK";
                break;
            case Leader.COMMIT:
                type = "COMMIT";
                break;
            case Leader.FOLLOWERINFO:
                type = "FOLLOWERINFO";
                break;
            case Leader.NEWLEADER:
                type = "NEWLEADER";
                break;
            case Leader.PING:
                type = "PING";
                break;
            case Leader.PROPOSAL:
                type = "PROPOSAL";
                TxnHeader hdr = new TxnHeader();
                try {
                    txn = SerializeUtils.deserializeTxn(p.getData(), hdr);
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }
                break;
            case Leader.REQUEST:
                type = "REQUEST";
                break;
            case Leader.REVALIDATE:
                type = "REVALIDATE";
                ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
                DataInputStream dis = new DataInputStream(bis);
                try {
                    long id = dis.readLong();
                    mess = " sessionid = " + id;
                } catch (IOException e) {
                    LOG.warn("Unexpected exception", e);
                }
                break;
            case Leader.UPTODATE:
                type = "UPTODATE";
                break;
            default:
                type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            ia = BinaryInputArchive.getArchive(new BufferedInputStream(sock.getInputStream()));
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);
            QuorumPacket var0 = new QuorumPacket();
            ia.readRecord(var0, "packet");
            if (var0.getType() != Leader.FOLLOWERINFO && var0.getType() != Leader.OBSERVERINFO) {
                LOG.error("First packet " + var0.toString() + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
            byte[] learnerInfoData = var0.getData();
            if (learnerInfoData != null) {
                if (learnerInfoData.length == 8) {
                    ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                    this.sid = bbsid.getLong();
                } else {
                    LearnerInfo li = new LearnerInfo();
                    ZooKeeperServer.byteBuffer2Record(ByteBuffer.wrap(learnerInfoData), li);
                    this.sid = li.getServerid();
                    this.version = li.getProtocolVersion();
                }
            } else {
                this.sid = leader.followerCounter.getAndDecrement();
            }
            LOG.info("Follower sid: " + this.sid + " : info : " + leader.self.quorumPeers.get(this.sid));
            if (var0.getType() == Leader.OBSERVERINFO) {
                learnerType = LearnerType.OBSERVER;
            }
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(var0.getZxid());
            long peerLastZxid;
            StateSummary ss = null;
            if (learnerType == LearnerType.PARTICIPANT) {
                long zxid = var0.getZxid();
                long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
                if (this.getVersion() < 0x10000) {
                    // we are going to have to extrapolate the epoch information
                    long epoch = ZxidUtils.getEpochFromZxid(zxid);
                    ss = new StateSummary(epoch, zxid);
                    // fake the message
                    leader.waitForEpochAck(this.getSid(), ss);
                } else {
                    byte[] ver = new byte[4];
                    ByteBuffer.wrap(ver).putInt(0x10000);
                    QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, ZxidUtils.makeZxid(newEpoch, 0), ver, null);
                    oa.writeRecord(newEpochPacket, "packet");
                    bufferedOutput.flush();
                    QuorumPacket ackEpochPacket = new QuorumPacket();
                    ia.readRecord(ackEpochPacket, "packet");
                    if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                        LOG.error(ackEpochPacket.toString() + " is not ACKEPOCH");
                        return;
                    }
                    ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                    ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                    leader.waitForEpochAck(this.getSid(), ss);
                }
                peerLastZxid = ss.getLastZxid();
            } else {
                peerLastZxid = var0.getZxid();
            }
            /* the default to send to the follower */
            int packetToSend = Leader.SNAP;
            long zxidToSend = 0;
            long leaderLastZxid = 0;
            /**
             * the packets that the follower needs to get updates from *
             */
            long updates = peerLastZxid;
            /* we are sending the diff check if we have proposals in memory to be able to 
             * send a diff to the 
             */
            ReentrantReadWriteLock lock = leader.zk.getZKDatabase().getLogLock();
            ReadLock rl = lock.readLock();
            try {
                rl.lock();
                final long maxCommittedLog = leader.zk.getZKDatabase().getmaxCommittedLog();
                final long minCommittedLog = leader.zk.getZKDatabase().getminCommittedLog();
                LOG.info("Synchronizing with Follower sid: " + this.sid + " maxCommittedLog =" + Long.toHexString(maxCommittedLog) + " minCommittedLog = " + Long.toHexString(minCommittedLog) + " peerLastZxid = " + Long.toHexString(peerLastZxid));
                LinkedList<Proposal> proposals = leader.zk.getZKDatabase().getCommittedLog();
                if (proposals.size() != 0) {
                    if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {
                        // proposal Id.
                        long prevProposalZxid = minCommittedLog;
                        // whether to expect a trunc or a diff
                        boolean firstPacket = true;
                        for (Proposal propose : proposals) {
                            // skip the proposals the peer already has
                            if (propose.packet.getZxid() <= peerLastZxid) {
                                prevProposalZxid = propose.packet.getZxid();
                                continue;
                            } else {
                                // in case the follower has some proposals that the leader doesn't
                                if (firstPacket) {
                                    firstPacket = false;
                                    // Does the peer have some proposals that the leader hasn't seen yet
                                    if (prevProposalZxid < peerLastZxid) {
                                        // send a trunc message before sending the diff
                                        packetToSend = Leader.TRUNC;
                                        LOG.info("Sending TRUNC");
                                        zxidToSend = prevProposalZxid;
                                        updates = zxidToSend;
                                    } else {
                                        // Just send the diff
                                        packetToSend = Leader.DIFF;
                                        LOG.info("Sending diff");
                                        zxidToSend = maxCommittedLog;
                                    }
                                }
                                queuePacket(propose.packet);
                                QuorumPacket qcommit = new QuorumPacket(Leader.COMMIT, propose.packet.getZxid(), null, null);
                                queuePacket(qcommit);
                            }
                        }
                    } else if (peerLastZxid > maxCommittedLog) {
                        packetToSend = Leader.TRUNC;
                        zxidToSend = maxCommittedLog;
                        updates = zxidToSend;
                    }
                } else {
                }
                leaderLastZxid = leader.startForwarding(this, updates);
                if (peerLastZxid == leaderLastZxid) {
                    // We are in sync so we'll do an empty diff
                    packetToSend = Leader.DIFF;
                    zxidToSend = leaderLastZxid;
                }
            } finally {
                rl.unlock();
            }
            QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, leaderLastZxid, null, null);
            oa.writeRecord(newLeaderQP, "packet");
            bufferedOutput.flush();
            // Need to set the zxidToSend to the latest zxid
            if (packetToSend == Leader.SNAP) {
                zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
            }
            oa.writeRecord(new QuorumPacket(packetToSend, zxidToSend, null, null), "packet");
            bufferedOutput.flush();
            /* if we are not truncating or sending a diff just send a snapshot */
            if (packetToSend == Leader.SNAP) {
                LOG.info("Sending snapshot last zxid of peer is 0x" + Long.toHexString(peerLastZxid) + " " + " zxid of leader is 0x" + Long.toHexString(leaderLastZxid) + "sent zxid of db as 0x" + Long.toHexString(zxidToSend));
                // Dump data to peer
                leader.zk.getZKDatabase().serializeSnapshot(oa);
                oa.writeString("BenWasHere", "signature");
            }
            bufferedOutput.flush();
            // 
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));
            // Start sending packets
            new Thread() {

                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            /*
             * Have to wait for the first ACK, wait until 
             * the leader is ready, and only then we can
             * start processing messages.
             */
            var0 = new QuorumPacket();
            ia.readRecord(var0, "packet");
            if (var0.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK");
                return;
            }
            leader.processAck(this.sid, var0.getZxid(), sock.getLocalSocketAddress());
            /*
             * Wait until leader starts up
             */
            synchronized (leader.zk) {
                while (!leader.zk.isRunning() && !this.isInterrupted()) {
                    leader.zk.wait(20);
                }
            }
            while (true) {
                var0 = new QuorumPacket();
                ia.readRecord(var0, "packet");
                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (var0.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', var0);
                }
                tickOfLastAck = leader.self.tick;
                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;
                switch(var0.getType()) {
                    case Leader.ACK:
                        if (this.learnerType == LearnerType.OBSERVER) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Received ACK from Observer  " + this.sid);
                            }
                        }
                        leader.processAck(this.sid, var0.getZxid(), sock.getLocalSocketAddress());
                        break;
                    case Leader.PING:
                        // Process the touches
                        ByteArrayInputStream bis = new ByteArrayInputStream(var0.getData());
                        DataInputStream dis = new DataInputStream(bis);
                        while (dis.available() > 0) {
                            long sess = dis.readLong();
                            int to = dis.readInt();
                            leader.zk.touch(sess, to);
                        }
                        break;
                    case Leader.REVALIDATE:
                        bis = new ByteArrayInputStream(var0.getData());
                        dis = new DataInputStream(bis);
                        long id = dis.readLong();
                        int to = dis.readInt();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        DataOutputStream dos = new DataOutputStream(bos);
                        dos.writeLong(id);
                        boolean valid = leader.zk.touch(id, to);
                        if (valid) {
                            try {
                                // owns the session
                                leader.zk.setOwner(id, this);
                            } catch (SessionExpiredException e) {
                                LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                            }
                        }
                        if (LOG.isTraceEnabled()) {
                            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "Session 0x" + Long.toHexString(id) + " is valid: " + valid);
                        }
                        dos.writeBoolean(valid);
                        var0.setData(bos.toByteArray());
                        queuedPackets.add(var0);
                        break;
                    case Leader.REQUEST:
                        bb = ByteBuffer.wrap(var0.getData());
                        sessionId = bb.getLong();
                        cxid = bb.getInt();
                        type = bb.getInt();
                        bb = bb.slice();
                        Request si;
                        if (type == OpCode.sync) {
                            si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, var0.getAuthinfo());
                        } else {
                            si = new Request(null, sessionId, cxid, type, bb, var0.getAuthinfo());
                        }
                        si.setOwner(this);
                        leader.zk.submitRequest(si);
                        break;
                    default:
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock " + "still open", e);
                // other side can see it being close
                try {
                    sock.close();
                } catch (IOException ie) {
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } finally {
            LOG.warn("******* GOODBYE " + (sock != null ? sock.getRemoteSocketAddress() : "<null>") + " ********");
            shutdown();
        }
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfLastAck() {
        return tickOfLastAck;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() {
        long id;
        synchronized (leader) {
            id = leader.lastProposed;
        }
        QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
        queuePacket(ping);
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() {
        return isAlive() && tickOfLastAck >= leader.self.tick - leader.self.syncLimit;
    }
}
