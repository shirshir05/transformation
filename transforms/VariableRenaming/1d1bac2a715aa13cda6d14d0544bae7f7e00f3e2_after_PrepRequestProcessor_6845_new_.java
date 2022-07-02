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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.jute.Record;
import org.apache.jute.BinaryOutputArchive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MultiTransactionRecord;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.PathUtils;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetACLRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.proto.CheckVersionRequest;
import org.apache.zookeeper.server.ZooKeeperServer.ChangeRecord;
import org.apache.zookeeper.server.auth.AuthenticationProvider;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.txn.CreateSessionTxn;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.CheckVersionTxn;
import org.apache.zookeeper.txn.Txn;
import org.apache.zookeeper.txn.MultiTxn;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This request processor is generally at the start of a RequestProcessor
 * change. It sets up any transactions associated with requests that change the
 * state of the system. It counts on ZooKeeperServer to update
 * outstandingRequests, so that it can take into account transactions that are
 * in the queue to be applied when generating a transaction.
 */
public class PrepRequestProcessor extends Thread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PrepRequestProcessor.class);

    static boolean skipACL;

    static {
        skipACL = System.getProperty("zookeeper.skipACL", "no").equals("yes");
        if (skipACL) {
            LOG.info("zookeeper.skipACL==\"yes\", ACL checks will be skipped");
        }
    }

    /**
     * this is only for testing purposes.
     * should never be useed otherwise
     */
    private static boolean failCreate = false;

    LinkedBlockingQueue<Request> submittedRequests = new LinkedBlockingQueue<Request>();

    RequestProcessor nextProcessor;

    ZooKeeperServer zks;

    public PrepRequestProcessor(ZooKeeperServer zks, RequestProcessor nextProcessor) {
        super("ProcessThread(sid:" + zks.getServerId() + " cport:" + zks.getClientPort() + "):");
        this.nextProcessor = nextProcessor;
        this.zks = zks;
    }

    /**
     * method for tests to set failCreate
     * @param b
     */
    public static void setFailCreate(boolean b) {
        failCreate = b;
    }

    @Override
    public void run() {
        try {
            while (true) {
                Request request = submittedRequests.take();
                long traceMask = ZooTrace.CLIENT_REQUEST_TRACE_MASK;
                if (request.type == OpCode.ping) {
                    traceMask = ZooTrace.CLIENT_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logRequest(LOG, traceMask, 'P', request, "");
                }
                if (Request.requestOfDeath == request) {
                    break;
                }
                pRequest(request);
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected interruption", e);
        }
        LOG.info("PrepRequestProcessor exited loop!");
    }

    ChangeRecord getRecordForPath(String path) throws KeeperException.NoNodeException {
        ChangeRecord lastChange = null;
        synchronized (zks.outstandingChanges) {
            lastChange = zks.outstandingChangesForPath.get(path);
            /*
            for (int i = 0; i < zks.outstandingChanges.size(); i++) {
                ChangeRecord c = zks.outstandingChanges.get(i);
                if (c.path.equals(path)) {
                    lastChange = c;
                }
            }
            */
            if (lastChange == null) {
                DataNode n = zks.getZKDatabase().getNode(path);
                if (n != null) {
                    Long acl;
                    Set<String> children;
                    synchronized (n) {
                        acl = n.acl;
                        children = n.getChildren();
                    }
                    lastChange = new ChangeRecord(-1, path, n.stat, children != null ? children.size() : 0, zks.getZKDatabase().convertLong(acl));
                }
            }
        }
        if (lastChange == null || lastChange.stat == null) {
            throw new KeeperException.NoNodeException(path);
        }
        return lastChange;
    }

    void addChangeRecord(ChangeRecord c) {
        synchronized (zks.outstandingChanges) {
            zks.outstandingChanges.add(c);
            zks.outstandingChangesForPath.put(c.path, c);
        }
    }

    /**
     * Grab current pending change records for each op in a multi-op.
     *
     * This is used inside MultiOp error code path to rollback in the event
     * of a failed multi-op.
     *
     * @param multiRequest
     */
    HashMap<String, ChangeRecord> getPendingChanges(MultiTransactionRecord multiRequest) {
        HashMap<String, ChangeRecord> pendingChangeRecords = new HashMap<String, ChangeRecord>();
        for (Op op : multiRequest) {
            String path = op.getPath();
            try {
                ChangeRecord cr = getRecordForPath(path);
                if (cr != null) {
                    pendingChangeRecords.put(path, cr);
                }
            } catch (KeeperException.NoNodeException e) {
            }
        }
        return pendingChangeRecords;
    }

    /**
     * Rollback pending changes records from a failed multi-op.
     *
     * If a multi-op fails, we can't leave any invalid change records we created
     * around. We also need to restore their prior value (if any) if their prior
     * value is still valid.
     *
     * @param zxid
     * @param pendingChangeRecords
     */
    void rollbackPendingChanges(long zxid, HashMap<String, ChangeRecord> pendingChangeRecords) {
        synchronized (zks.outstandingChanges) {
            // Grab a list iterator starting at the END of the list so we can iterate in reverse
            ListIterator<ChangeRecord> iter = zks.outstandingChanges.listIterator(zks.outstandingChanges.size());
            while (iter.hasPrevious()) {
                ChangeRecord c = iter.previous();
                if (c.zxid == zxid) {
                    iter.remove();
                    zks.outstandingChangesForPath.remove(c.path);
                } else {
                    break;
                }
            }
            boolean empty = zks.outstandingChanges.isEmpty();
            long firstZxid = 0;
            if (!empty) {
                firstZxid = zks.outstandingChanges.get(0).zxid;
            }
            Iterator<ChangeRecord> priorIter = pendingChangeRecords.values().iterator();
            while (priorIter.hasNext()) {
                ChangeRecord c = priorIter.next();
                /* Don't apply any prior change records less than firstZxid */
                if (!empty && (c.zxid < firstZxid)) {
                    continue;
                }
                zks.outstandingChangesForPath.put(c.path, c);
            }
        }
    }

    static void checkACL(ZooKeeperServer zks, List<ACL> acl, int perm, List<Id> ids) throws KeeperException.NoAuthException {
        if (skipACL) {
            return;
        }
        if (acl == null || acl.size() == 0) {
            return;
        }
        for (Id authId : ids) {
            if (authId.getScheme().equals("super")) {
                return;
            }
        }
        for (ACL a : acl) {
            Id id = a.getId();
            if ((a.getPerms() & perm) != 0) {
                if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
                    return;
                }
                AuthenticationProvider ap = ProviderRegistry.getProvider(id.getScheme());
                if (ap != null) {
                    for (Id authId : ids) {
                        if (authId.getScheme().equals(id.getScheme()) && ap.matches(authId.getId(), id.getId())) {
                            return;
                        }
                    }
                }
            }
        }
        throw new KeeperException.NoAuthException();
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param type
     * @param zxid
     * @param request
     * @param record
     */
    @SuppressWarnings("unchecked")
    protected void pRequest2Txn(int type, long zxid, Request var2, Record record) throws KeeperException {
        var2.hdr = new TxnHeader(var2.sessionId, var2.cxid, zxid, zks.getTime(), type);
        switch(type) {
            case OpCode.create:
                zks.sessionTracker.checkSession(var2.sessionId, var2.getOwner());
                CreateRequest createRequest = (CreateRequest) record;
                String path = createRequest.getPath();
                int lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1 || failCreate) {
                    LOG.info("Invalid path " + path + " with session 0x" + Long.toHexString(var2.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                List<ACL> listACL = removeDuplicates(createRequest.getAcl());
                if (!fixupACL(var2.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                String parentPath = path.substring(0, lastSlash);
                ChangeRecord parentRecord = getRecordForPath(parentPath);
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.CREATE, var2.authInfo);
                int parentCVersion = parentRecord.stat.getCversion();
                CreateMode createMode = CreateMode.fromFlag(createRequest.getFlags());
                if (createMode.isSequential()) {
                    path = path + String.format("%010d", parentCVersion);
                }
                try {
                    PathUtils.validatePath(path);
                } catch (IllegalArgumentException ie) {
                    LOG.info("Invalid path " + path + " with session 0x" + Long.toHexString(var2.sessionId));
                    throw new KeeperException.BadArgumentsException(path);
                }
                try {
                    if (getRecordForPath(path) != null) {
                        throw new KeeperException.NodeExistsException(path);
                    }
                } catch (KeeperException.NoNodeException e) {
                }
                boolean ephemeralParent = parentRecord.stat.getEphemeralOwner() != 0;
                if (ephemeralParent) {
                    throw new KeeperException.NoChildrenForEphemeralsException(path);
                }
                int newCversion = parentRecord.stat.getCversion() + 1;
                var2.txn = new CreateTxn(path, createRequest.getData(), listACL, createMode.isEphemeral(), newCversion);
                StatPersisted s = new StatPersisted();
                if (createMode.isEphemeral()) {
                    s.setEphemeralOwner(var2.sessionId);
                }
                parentRecord = parentRecord.duplicate(var2.hdr.getZxid());
                parentRecord.childCount++;
                parentRecord.stat.setCversion(newCversion);
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(var2.hdr.getZxid(), path, s, 0, listACL));
                break;
            case OpCode.delete:
                zks.sessionTracker.checkSession(var2.sessionId, var2.getOwner());
                DeleteRequest deleteRequest = (DeleteRequest) record;
                path = deleteRequest.getPath();
                lastSlash = path.lastIndexOf('/');
                if (lastSlash == -1 || path.indexOf('\0') != -1 || zks.getZKDatabase().isSpecialPath(path)) {
                    throw new KeeperException.BadArgumentsException(path);
                }
                parentPath = path.substring(0, lastSlash);
                parentRecord = getRecordForPath(parentPath);
                ChangeRecord nodeRecord = getRecordForPath(path);
                checkACL(zks, parentRecord.acl, ZooDefs.Perms.DELETE, var2.authInfo);
                int version = deleteRequest.getVersion();
                if (version != -1 && nodeRecord.stat.getVersion() != version) {
                    throw new KeeperException.BadVersionException(path);
                }
                if (nodeRecord.childCount > 0) {
                    throw new KeeperException.NotEmptyException(path);
                }
                var2.txn = new DeleteTxn(path);
                parentRecord = parentRecord.duplicate(var2.hdr.getZxid());
                parentRecord.childCount--;
                addChangeRecord(parentRecord);
                addChangeRecord(new ChangeRecord(var2.hdr.getZxid(), path, null, -1, null));
                break;
            case OpCode.setData:
                zks.sessionTracker.checkSession(var2.sessionId, var2.getOwner());
                SetDataRequest setDataRequest = (SetDataRequest) record;
                path = setDataRequest.getPath();
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.WRITE, var2.authInfo);
                version = setDataRequest.getVersion();
                int currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                var2.txn = new SetDataTxn(path, setDataRequest.getData(), version);
                nodeRecord = nodeRecord.duplicate(var2.hdr.getZxid());
                nodeRecord.stat.setVersion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.setACL:
                zks.sessionTracker.checkSession(var2.sessionId, var2.getOwner());
                SetACLRequest setAclRequest = (SetACLRequest) record;
                path = setAclRequest.getPath();
                listACL = removeDuplicates(setAclRequest.getAcl());
                if (!fixupACL(var2.authInfo, listACL)) {
                    throw new KeeperException.InvalidACLException(path);
                }
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.ADMIN, var2.authInfo);
                version = setAclRequest.getVersion();
                currentVersion = nodeRecord.stat.getAversion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                var2.txn = new SetACLTxn(path, listACL, version);
                nodeRecord = nodeRecord.duplicate(var2.hdr.getZxid());
                nodeRecord.stat.setAversion(version);
                addChangeRecord(nodeRecord);
                break;
            case OpCode.createSession:
                var2.var2.rewind();
                int to = var2.var2.getInt();
                var2.txn = new CreateSessionTxn(to);
                var2.var2.rewind();
                zks.sessionTracker.addSession(var2.sessionId, to);
                zks.setOwner(var2.sessionId, var2.getOwner());
                break;
            case OpCode.closeSession:
                // zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                HashSet<String> es = zks.getZKDatabase().getEphemerals(var2.sessionId);
                synchronized (zks.outstandingChanges) {
                    for (ChangeRecord c : zks.outstandingChanges) {
                        if (c.stat == null) {
                            // Doing a delete
                            es.remove(c.path);
                        } else if (c.stat.getEphemeralOwner() == var2.sessionId) {
                            es.add(c.path);
                        }
                    }
                    for (String path2Delete : es) {
                        addChangeRecord(new ChangeRecord(var2.hdr.getZxid(), path2Delete, null, 0, null));
                    }
                }
                LOG.info("Processed session termination for sessionid: 0x" + Long.toHexString(var2.sessionId));
                break;
            case OpCode.check:
                zks.sessionTracker.checkSession(var2.sessionId, var2.getOwner());
                CheckVersionRequest checkVersionRequest = (CheckVersionRequest) record;
                path = checkVersionRequest.getPath();
                nodeRecord = getRecordForPath(path);
                checkACL(zks, nodeRecord.acl, ZooDefs.Perms.READ, var2.authInfo);
                version = checkVersionRequest.getVersion();
                currentVersion = nodeRecord.stat.getVersion();
                if (version != -1 && version != currentVersion) {
                    throw new KeeperException.BadVersionException(path);
                }
                version = currentVersion + 1;
                var2.txn = new CheckVersionTxn(path, version);
                break;
        }
    }

    /**
     * This method will be called inside the ProcessRequestThread, which is a
     * singleton, so there will be a single thread calling this code.
     *
     * @param request
     */
    @SuppressWarnings("unchecked")
    protected void pRequest(Request request) {
        // request.type + " id = 0x" + Long.toHexString(request.sessionId));
        request.hdr = null;
        request.txn = null;
        try {
            switch(request.type) {
                case OpCode.create:
                    CreateRequest createRequest = new CreateRequest();
                    ZooKeeperServer.byteBuffer2Record(request.request, createRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, createRequest);
                    break;
                case OpCode.delete:
                    DeleteRequest deleteRequest = new DeleteRequest();
                    ZooKeeperServer.byteBuffer2Record(request.request, deleteRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, deleteRequest);
                    break;
                case OpCode.setData:
                    SetDataRequest setDataRequest = new SetDataRequest();
                    ZooKeeperServer.byteBuffer2Record(request.request, setDataRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, setDataRequest);
                    break;
                case OpCode.setACL:
                    SetACLRequest setAclRequest = new SetACLRequest();
                    ZooKeeperServer.byteBuffer2Record(request.request, setAclRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, setAclRequest);
                    break;
                case OpCode.check:
                    CheckVersionRequest checkRequest = new CheckVersionRequest();
                    ZooKeeperServer.byteBuffer2Record(request.request, checkRequest);
                    pRequest2Txn(request.type, zks.getNextZxid(), request, checkRequest);
                    break;
                case OpCode.multi:
                    MultiTransactionRecord multiRequest = new MultiTransactionRecord();
                    ZooKeeperServer.byteBuffer2Record(request.request, multiRequest);
                    List<Txn> txns = new ArrayList<Txn>();
                    // Each op in a multi-op must have the same zxid!
                    long zxid = zks.getNextZxid();
                    KeeperException ke = null;
                    // Store off current pending change records in case we need to rollback
                    HashMap<String, ChangeRecord> pendingChanges = getPendingChanges(multiRequest);
                    int index = 0;
                    for (Op op : multiRequest) {
                        Record subrequest = op.toRequestRecord();
                        /* If we've already failed one of the ops, don't bother
                     * trying the rest as we know it's going to fail and it
                     * would be confusing in the logfiles.
                     */
                        if (ke != null) {
                            request.hdr.setType(OpCode.error);
                            request.txn = new ErrorTxn(Code.RUNTIMEINCONSISTENCY.intValue());
                        } else /* Prep the request and convert to a Txn */
                        {
                            try {
                                pRequest2Txn(op.getType(), zxid, request, subrequest);
                            } catch (KeeperException e) {
                                if (ke == null) {
                                    ke = e;
                                }
                                request.hdr.setType(OpCode.error);
                                request.txn = new ErrorTxn(e.code().intValue());
                                LOG.error(">>>> Got user-level KeeperException when processing " + request.toString() + " Error Path:" + e.getPath() + " Error:" + e.getMessage());
                                LOG.error(">>>> ABORTING remaing MultiOp ops");
                                request.setException(e);
                                /* Rollback change records from failed multi-op */
                                rollbackPendingChanges(zxid, pendingChanges);
                            }
                        }
                        // not sure how else to get the txn stored into our list.
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
                        request.txn.serialize(boa, "request");
                        ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
                        txns.add(new Txn(request.hdr.getType(), bb.array()));
                        index++;
                    }
                    request.hdr = new TxnHeader(request.sessionId, request.cxid, zxid, zks.getTime(), request.type);
                    request.txn = new MultiTxn(txns);
                    break;
                // create/close session don't require request record
                case OpCode.createSession:
                case OpCode.closeSession:
                    pRequest2Txn(request.type, zks.getNextZxid(), request, null);
                    break;
                // All the rest don't need to create a Txn - just verify session
                case OpCode.sync:
                case OpCode.exists:
                case OpCode.getData:
                case OpCode.getACL:
                case OpCode.getChildren:
                case OpCode.getChildren2:
                case OpCode.ping:
                case OpCode.setWatches:
                    zks.sessionTracker.checkSession(request.sessionId, request.getOwner());
                    break;
            }
        } catch (KeeperException e) {
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(e.code().intValue());
            }
            LOG.info("Got user-level KeeperException when processing " + request.toString() + " Error Path:" + e.getPath() + " Error:" + e.getMessage());
            request.setException(e);
        } catch (Exception e) {
            // error to the user
            LOG.error("Failed to process " + request, e);
            StringBuilder sb = new StringBuilder();
            ByteBuffer bb = request.request;
            if (bb != null) {
                bb.rewind();
                while (bb.hasRemaining()) {
                    sb.append(Integer.toHexString(bb.get() & 0xff));
                }
            } else {
                sb.append("request buffer is null");
            }
            LOG.error("Dumping request buffer: 0x" + sb.toString());
            if (request.hdr != null) {
                request.hdr.setType(OpCode.error);
                request.txn = new ErrorTxn(Code.MARSHALLINGERROR.intValue());
            }
        }
        request.zxid = zks.getZxid();
        nextProcessor.processRequest(request);
    }

    private List<ACL> removeDuplicates(List<ACL> acl) {
        ArrayList<ACL> retval = new ArrayList<ACL>();
        Iterator<ACL> it = acl.iterator();
        while (it.hasNext()) {
            ACL a = it.next();
            if (retval.contains(a) == false) {
                retval.add(a);
            }
        }
        return retval;
    }

    /**
     * This method checks out the acl making sure it isn't null or empty,
     * it has valid schemes and ids, and expanding any relative ids that
     * depend on the requestor's authentication information.
     *
     * @param authInfo list of ACL IDs associated with the client connection
     * @param acl list of ACLs being assigned to the node (create or setACL operation)
     * @return
     */
    private boolean fixupACL(List<Id> authInfo, List<ACL> acl) {
        if (skipACL) {
            return true;
        }
        if (acl == null || acl.size() == 0) {
            return false;
        }
        Iterator<ACL> it = acl.iterator();
        LinkedList<ACL> toAdd = null;
        while (it.hasNext()) {
            ACL a = it.next();
            Id id = a.getId();
            if (id.getScheme().equals("world") && id.getId().equals("anyone")) {
            } else if (id.getScheme().equals("auth")) {
                // authenticated ids of the requestor
                it.remove();
                if (toAdd == null) {
                    toAdd = new LinkedList<ACL>();
                }
                boolean authIdValid = false;
                for (Id cid : authInfo) {
                    AuthenticationProvider ap = ProviderRegistry.getProvider(cid.getScheme());
                    if (ap == null) {
                        LOG.error("Missing AuthenticationProvider for " + cid.getScheme());
                    } else if (ap.isAuthenticated()) {
                        authIdValid = true;
                        toAdd.add(new ACL(a.getPerms(), cid));
                    }
                }
                if (!authIdValid) {
                    return false;
                }
            } else {
                AuthenticationProvider ap = ProviderRegistry.getProvider(id.getScheme());
                if (ap == null) {
                    return false;
                }
                if (!ap.isValid(id.getId())) {
                    return false;
                }
            }
        }
        if (toAdd != null) {
            for (ACL a : toAdd) {
                acl.add(a);
            }
        }
        return acl.size() > 0;
    }

    public void processRequest(Request request) {
        // request.addRQRec(">prep="+zks.outstandingChanges.size());
        submittedRequests.add(request);
    }

    public void shutdown() {
        LOG.info("Shutting down");
        submittedRequests.clear();
        submittedRequests.add(Request.requestOfDeath);
        nextProcessor.shutdown();
    }
}