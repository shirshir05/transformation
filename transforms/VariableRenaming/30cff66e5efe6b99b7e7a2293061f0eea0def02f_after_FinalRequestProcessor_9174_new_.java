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
package org.apache.zookeeper.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateResponse;
import org.apache.zookeeper.proto.ExistsRequest;
import org.apache.zookeeper.proto.ExistsResponse;
import org.apache.zookeeper.proto.GetACLRequest;
import org.apache.zookeeper.proto.GetACLResponse;
import org.apache.zookeeper.proto.GetChildren2Request;
import org.apache.zookeeper.proto.GetChildren2Response;
import org.apache.zookeeper.proto.GetChildrenRequest;
import org.apache.zookeeper.proto.GetChildrenResponse;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.SetACLResponse;
import org.apache.zookeeper.proto.SetDataResponse;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.proto.SyncRequest;
import org.apache.zookeeper.proto.SyncResponse;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.ErrorTxn;

/**
 * This Request processor actually applies any transaction associated with a
 * request and services any queries. It is always at the end of a
 * RequestProcessor chain (hence the name), so it does not have a nextProcessor
 * member.
 *
 * This RequestProcessor counts on ZooKeeperServer to populate the
 * outstandingRequests member of ZooKeeperServer.
 */
public class FinalRequestProcessor implements RequestProcessor {

    private static final Logger LOG = Logger.getLogger(FinalRequestProcessor.class);

    ZooKeeperServer zks;

    public FinalRequestProcessor(ZooKeeperServer zks) {
        this.zks = zks;
    }

    public void processRequest(Request var0) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Processing request:: " + var0);
        }
        // request.addRQRec(">final");
        long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
        if (var0.type == OpCode.ping) {
            traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
        }
        if (LOG.isTraceEnabled()) {
            ZooTrace.logRequest(LOG, traceMask, 'E', var0, "");
        }
        ProcessTxnResult rc = null;
        synchronized (zks.outstandingChanges) {
            while (!zks.outstandingChanges.isEmpty() && zks.outstandingChanges.get(0).zxid <= var0.zxid) {
                ChangeRecord cr = zks.outstandingChanges.remove(0);
                if (cr.zxid < var0.zxid) {
                    LOG.warn("Zxid outstanding " + cr.zxid + " is less than current " + var0.zxid);
                }
                if (zks.outstandingChangesForPath.get(cr.path) == cr) {
                    zks.outstandingChangesForPath.remove(cr.path);
                }
            }
            if (var0.hdr != null) {
                rc = zks.getZKDatabase().processTxn(var0.hdr, var0.txn);
                if (var0.type == OpCode.createSession) {
                    if (var0.txn instanceof CreateSessionTxn) {
                        CreateSessionTxn cst = (CreateSessionTxn) var0.txn;
                        zks.sessionTracker.addSession(var0.sessionId, cst.getTimeOut());
                    } else {
                        LOG.warn("*****>>>>> Got " + var0.txn.getClass() + " " + var0.txn.toString());
                    }
                } else if (var0.type == OpCode.closeSession) {
                    zks.sessionTracker.removeSession(var0.sessionId);
                }
            }
            // do not add non quorum packets to the queue.
            if (Request.isQuorum(var0.type)) {
                zks.getZKDatabase().addCommittedProposal(var0);
            }
        }
        if (var0.hdr != null && var0.hdr.getType() == OpCode.closeSession) {
            ServerCnxnFactory scxn = zks.getServerCnxnFactory();
            // we might just be playing diffs from the leader
            if (scxn != null && var0.cnxn == null) {
                // in the switch block below
                scxn.closeSession(var0.sessionId);
                return;
            }
        }
        if (var0.cnxn == null) {
            return;
        }
        ServerCnxn cnxn = var0.cnxn;
        String lastOp = "NA";
        zks.decInProcess();
        Code err = Code.OK;
        Record rsp = null;
        boolean closeSession = false;
        try {
            if (var0.hdr != null && var0.hdr.getType() == OpCode.error) {
                throw KeeperException.create(KeeperException.Code.get(((ErrorTxn) var0.txn).getErr()));
            }
            KeeperException ke = var0.getException();
            if (ke != null) {
                throw ke;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(var0);
            }
            switch(var0.type) {
                case OpCode.ping:
                    {
                        zks.serverStats().updateLatency(var0.createTime);
                        lastOp = "PING";
                        cnxn.updateStatsForResponse(var0.cxid, var0.zxid, lastOp, var0.createTime, System.currentTimeMillis());
                        cnxn.sendResponse(new ReplyHeader(-2, zks.getZKDatabase().getDataTreeLastProcessedZxid(), 0), null, "response");
                        return;
                    }
                case OpCode.createSession:
                    {
                        zks.serverStats().updateLatency(var0.createTime);
                        lastOp = "SESS";
                        cnxn.updateStatsForResponse(var0.cxid, var0.zxid, lastOp, var0.createTime, System.currentTimeMillis());
                        zks.finishSessionInit(var0.cnxn, true);
                        return;
                    }
                case OpCode.create:
                    {
                        lastOp = "CREA";
                        rsp = new CreateResponse(rc.path);
                        err = Code.get(rc.err);
                        break;
                    }
                case OpCode.delete:
                    {
                        lastOp = "DELE";
                        err = Code.get(rc.err);
                        break;
                    }
                case OpCode.setData:
                    {
                        lastOp = "SETD";
                        rsp = new SetDataResponse(rc.stat);
                        err = Code.get(rc.err);
                        break;
                    }
                case OpCode.setACL:
                    {
                        lastOp = "SETA";
                        rsp = new SetACLResponse(rc.stat);
                        err = Code.get(rc.err);
                        break;
                    }
                case OpCode.closeSession:
                    {
                        lastOp = "CLOS";
                        closeSession = true;
                        err = Code.get(rc.err);
                        break;
                    }
                case OpCode.sync:
                    {
                        lastOp = "SYNC";
                        SyncRequest syncRequest = new SyncRequest();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, syncRequest);
                        rsp = new SyncResponse(syncRequest.getPath());
                        break;
                    }
                case OpCode.exists:
                    {
                        lastOp = "EXIS";
                        // TODO we need to figure out the security requirement for this!
                        ExistsRequest existsRequest = new ExistsRequest();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, existsRequest);
                        String path = existsRequest.getPath();
                        if (path.indexOf('\0') != -1) {
                            throw new KeeperException.BadArgumentsException();
                        }
                        Stat stat = zks.getZKDatabase().statNode(path, existsRequest.getWatch() ? cnxn : null);
                        rsp = new ExistsResponse(stat);
                        break;
                    }
                case OpCode.getData:
                    {
                        lastOp = "GETD";
                        GetDataRequest getDataRequest = new GetDataRequest();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, getDataRequest);
                        DataNode n = zks.getZKDatabase().getNode(getDataRequest.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclL;
                        synchronized (n) {
                            aclL = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclL), ZooDefs.Perms.READ, var0.authInfo);
                        Stat stat = new Stat();
                        byte[] b = zks.getZKDatabase().getData(getDataRequest.getPath(), stat, getDataRequest.getWatch() ? cnxn : null);
                        rsp = new GetDataResponse(b, stat);
                        break;
                    }
                case OpCode.setWatches:
                    {
                        lastOp = "SETW";
                        SetWatches setWatches = new SetWatches();
                        // XXX We really should NOT need this!!!!
                        var0.var0.rewind();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, setWatches);
                        long relativeZxid = setWatches.getRelativeZxid();
                        zks.getZKDatabase().setWatches(relativeZxid, setWatches.getDataWatches(), setWatches.getExistWatches(), setWatches.getChildWatches(), cnxn);
                        break;
                    }
                case OpCode.getACL:
                    {
                        lastOp = "GETA";
                        GetACLRequest getACLRequest = new GetACLRequest();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, getACLRequest);
                        Stat stat = new Stat();
                        List<ACL> acl = zks.getZKDatabase().getACL(getACLRequest.getPath(), stat);
                        rsp = new GetACLResponse(acl, stat);
                        break;
                    }
                case OpCode.getChildren:
                    {
                        lastOp = "GETC";
                        GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, getChildrenRequest);
                        DataNode n = zks.getZKDatabase().getNode(getChildrenRequest.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclG;
                        synchronized (n) {
                            aclG = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ, var0.authInfo);
                        List<String> children = zks.getZKDatabase().getChildren(getChildrenRequest.getPath(), null, getChildrenRequest.getWatch() ? cnxn : null);
                        rsp = new GetChildrenResponse(children);
                        break;
                    }
                case OpCode.getChildren2:
                    {
                        lastOp = "GETC";
                        GetChildren2Request getChildren2Request = new GetChildren2Request();
                        ZooKeeperServer.byteBuffer2Record(var0.var0, getChildren2Request);
                        Stat stat = new Stat();
                        DataNode n = zks.getZKDatabase().getNode(getChildren2Request.getPath());
                        if (n == null) {
                            throw new KeeperException.NoNodeException();
                        }
                        Long aclG;
                        synchronized (n) {
                            aclG = n.acl;
                        }
                        PrepRequestProcessor.checkACL(zks, zks.getZKDatabase().convertLong(aclG), ZooDefs.Perms.READ, var0.authInfo);
                        List<String> children = zks.getZKDatabase().getChildren(getChildren2Request.getPath(), stat, getChildren2Request.getWatch() ? cnxn : null);
                        rsp = new GetChildren2Response(children, stat);
                        break;
                    }
            }
        } catch (SessionMovedException e) {
            // recently attached (and therefore invalid SESSION MOVED generated)
            cnxn.sendCloseSession();
            return;
        } catch (KeeperException e) {
            err = e.code();
        } catch (Exception e) {
            // error to the user
            LOG.error("Failed to process " + var0, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = var0.var0;
            bb.rewind();
            while (bb.hasRemaining()) {
                sb.append(Integer.toHexString(bb.get() & 0xff));
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            err = Code.MARSHALLINGERROR;
        }
        ReplyHeader hdr = new ReplyHeader(var0.cxid, var0.zxid, err.intValue());
        zks.serverStats().updateLatency(var0.createTime);
        cnxn.updateStatsForResponse(var0.cxid, var0.zxid, lastOp, var0.createTime, System.currentTimeMillis());
        try {
            cnxn.sendResponse(hdr, rsp, "response");
            if (closeSession) {
                cnxn.sendCloseSession();
            }
        } catch (IOException e) {
            LOG.error("FIXMSG", e);
        }
    }

    public void shutdown() {
        // we are the final link in the chain
        LOG.info("shutdown of request processor complete");
    }
}
