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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.jute.Index;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.txn.CreateTxn;
import org.apache.zookeeper.txn.DeleteTxn;
import org.apache.zookeeper.txn.ErrorTxn;
import org.apache.zookeeper.txn.SetACLTxn;
import org.apache.zookeeper.txn.SetDataTxn;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * This class maintains the tree data structure. It doesn't have any networking
 * or client connection code in it so that it can be tested in a stand alone
 * way.
 * <p>
 * The tree maintains two parallel data structures: a hashtable that maps from
 * full paths to DataNodes and a tree of DataNodes. All accesses to a path is
 * through the hashtable. The tree is traversed only when serializing to disk.
 */
public class DataTree {

    private static final Logger LOG = Logger.getLogger(DataTree.class);

    /**
     * This hashtable provides a fast lookup to the datanodes. The tree is the
     * source of truth and is where all the locking occurs
     */
    private ConcurrentHashMap<String, DataNode> nodes = new ConcurrentHashMap<String, DataNode>();

    private WatchManager dataWatches = new WatchManager();

    private WatchManager childWatches = new WatchManager();

    /**
     * This hashtable lists the paths of the ephemeral nodes of a session.
     */
    private Map<Long, HashSet<String>> ephemerals = new ConcurrentHashMap<Long, HashSet<String>>();

    /**
     * this is map from longs to acl's. It saves acl's being stored
     * for each datanode.
     */
    public Map<Long, List<ACL>> longKeyMap = new HashMap<Long, List<ACL>>();

    /**
     * this a map from acls to long.
     */
    public Map<List<ACL>, Long> aclKeyMap = new HashMap<List<ACL>, Long>();

    /**
     * these are the number of acls
     * that we have in the datatree
     */
    protected long aclIndex = 0;

    /**
     * A debug string *
     */
    private String debug = "debug";

    @SuppressWarnings("unchecked")
    public HashSet<String> getEphemerals(long sessionId) {
        HashSet<String> retv = ephemerals.get(sessionId);
        if (retv == null) {
            return new HashSet<String>();
        }
        HashSet<String> cloned = null;
        synchronized (retv) {
            cloned = (HashSet<String>) retv.clone();
        }
        return cloned;
    }

    public Map<Long, HashSet<String>> getEphemeralsMap() {
        return ephemerals;
    }

    public void setEphemerals(Map<Long, HashSet<String>> ephemerals) {
        this.ephemerals = ephemerals;
    }

    private long incrementIndex() {
        return ++aclIndex;
    }

    /**
     * compare two list of acls. if there elements are in the same
     * order and the same size then return true else return false
     * @param lista the list to be compared
     * @param listb the list to be compared
     * @return true if and only if the lists are of the same size
     * and the elements are in the same order in lista and listb
     */
    private boolean listACLEquals(List<ACL> lista, List<ACL> listb) {
        if (lista.size() != listb.size()) {
            return false;
        }
        for (int i = 0; i < lista.size(); i++) {
            ACL a = lista.get(i);
            ACL b = listb.get(i);
            if (!a.equals(b)) {
                return false;
            }
        }
        return true;
    }

    /**
     * converts the list of acls to a list of
     * longs.
     * @param acls
     * @return a list of longs that map to the acls
     */
    public synchronized Long convertAcls(List<ACL> acls) {
        if (acls == null)
            return -1L;
        // get the value from the map
        Long ret = aclKeyMap.get(acls);
        // could not find the map
        if (ret != null)
            return ret;
        long val = incrementIndex();
        longKeyMap.put(val, acls);
        aclKeyMap.put(acls, val);
        return val;
    }

    /**
     * converts a list of longs to a list of acls.
     * @param longs the list of longs
     * @return a list of ACLs that map to longs
     */
    public synchronized List<ACL> convertLong(Long longVal) {
        if (longVal == null || longVal == -1L)
            return null;
        List<ACL> acls = longKeyMap.get(longVal);
        if (acls == null) {
            LOG.error("ERROR: ACL not available for long " + longVal);
            throw new RuntimeException("Failed to fetch acls for " + longVal);
        }
        return acls;
    }

    public Collection<Long> getSessions() {
        return ephemerals.keySet();
    }

    /**
     * just an accessor method to allow raw creation
     * of datatree's from a bunch of datanodes
     * @param path the path of the datanode
     * @param node the datanode corresponding to this
     * path
     */
    public void addDataNode(String path, DataNode node) {
        nodes.put(path, node);
    }

    public DataNode getNode(String path) {
        return nodes.get(path);
    }

    public int getNodeCount() {
        return nodes.size();
    }

    public int getWatchCount() {
        return dataWatches.size() + childWatches.size();
    }

    /**
     * This is a pointer to the root of the DataTree. It is the source of truth,
     * but we usually use the nodes hashmap to find nodes in the tree.
     */
    private DataNode root = new DataNode(null, new byte[0], -1L, new StatPersisted());

    public DataTree() {
        /* Rather than fight it, let root have an alias */
        nodes.put("", root);
        nodes.put("/", root);
    }

    static public void copyStatPersisted(StatPersisted from, StatPersisted to) {
        to.setAversion(from.getAversion());
        to.setCtime(from.getCtime());
        to.setCversion(from.getCversion());
        to.setCzxid(from.getCzxid());
        to.setMtime(from.getMtime());
        to.setMzxid(from.getMzxid());
        to.setPzxid(from.getPzxid());
        to.setVersion(from.getVersion());
        to.setEphemeralOwner(from.getEphemeralOwner());
    }

    static public void copyStat(Stat from, Stat var1) {
        var1.setAversion(from.getAversion());
        var1.setCtime(from.getCtime());
        var1.setCversion(from.getCversion());
        var1.setCzxid(from.getCzxid());
        var1.setMtime(from.getMtime());
        var1.setMzxid(from.getMzxid());
        var1.setPzxid(from.getPzxid());
        var1.setVersion(from.getVersion());
        var1.setEphemeralOwner(from.getEphemeralOwner());
        var1.setDataLength(from.getDataLength());
        var1.setNumChildren(from.getNumChildren());
    }

    /**
     * @param path
     * @param data
     * @param acl
     * @param ephemeralOwner
     *                the session id that owns this node. -1 indicates this is
     *                not an ephemeral node.
     * @param zxid
     * @param time
     * @return the patch of the created node
     * @throws KeeperException
     */
    public String createNode(String path, byte[] data, List<ACL> acl, long ephemeralOwner, long zxid, long time) throws KeeperException.NoNodeException, KeeperException.NodeExistsException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash);
        String childName = path.substring(lastSlash + 1);
        StatPersisted stat = new StatPersisted();
        stat.setCtime(time);
        stat.setMtime(time);
        stat.setCzxid(zxid);
        stat.setMzxid(zxid);
        stat.setPzxid(zxid);
        stat.setVersion(0);
        stat.setAversion(0);
        stat.setEphemeralOwner(ephemeralOwner);
        DataNode parent = nodes.get(parentName);
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            if (parent.children.contains(childName)) {
                throw new KeeperException.NodeExistsException();
            }
            int cver = parent.stat.getCversion();
            cver++;
            parent.stat.setCversion(cver);
            parent.stat.setPzxid(zxid);
            Long longval = convertAcls(acl);
            DataNode child = new DataNode(parent, data, longval, stat);
            parent.children.add(childName);
            nodes.put(path, child);
            if (ephemeralOwner != 0) {
                HashSet<String> list = ephemerals.get(ephemeralOwner);
                if (list == null) {
                    list = new HashSet<String>();
                    ephemerals.put(ephemeralOwner, list);
                }
                synchronized (list) {
                    list.add(path);
                }
            }
        }
        dataWatches.triggerWatch(path, Event.EventType.NodeCreated);
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName, Event.EventType.NodeChildrenChanged);
        return path;
    }

    /**
     * remove the path from the datatree
     * @param path the path to of the node to be deleted
     * @param zxid the current zxid
     * @throws KeeperException.NoNodeException
     */
    public void deleteNode(String path, long zxid) throws KeeperException.NoNodeException {
        int lastSlash = path.lastIndexOf('/');
        String parentName = path.substring(0, lastSlash);
        String childName = path.substring(lastSlash + 1);
        DataNode node = nodes.get(path);
        if (node == null) {
            throw new KeeperException.NoNodeException();
        }
        nodes.remove(path);
        DataNode parent = nodes.get(parentName);
        if (parent == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (parent) {
            parent.children.remove(childName);
            parent.stat.setCversion(parent.stat.getCversion() + 1);
            parent.stat.setPzxid(zxid);
            long eowner = node.stat.getEphemeralOwner();
            if (eowner != 0) {
                HashSet<String> nodes = ephemerals.get(eowner);
                if (nodes != null) {
                    synchronized (nodes) {
                        nodes.remove(path);
                    }
                }
            }
            node.parent = null;
        }
        ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK, "dataWatches.triggerWatch " + path);
        ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK, "childWatches.triggerWatch " + parentName);
        Set<Watcher> processed = dataWatches.triggerWatch(path, EventType.NodeDeleted);
        childWatches.triggerWatch(path, EventType.NodeDeleted, processed);
        childWatches.triggerWatch(parentName.equals("") ? "/" : parentName, EventType.NodeChildrenChanged);
    }

    public Stat setData(String path, byte[] data, int version, long zxid, long time) throws KeeperException.NoNodeException {
        Stat s = new Stat();
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.data = data;
            n.stat.setMtime(time);
            n.stat.setMzxid(zxid);
            n.stat.setVersion(version);
            n.copyStat(s);
        }
        dataWatches.triggerWatch(path, EventType.NodeDataChanged);
        return s;
    }

    public byte[] getData(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            if (watcher != null) {
                dataWatches.addWatch(path, watcher);
            }
            return n.data;
        }
    }

    public Stat statNode(String path, Watcher watcher) throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (watcher != null) {
            dataWatches.addWatch(path, watcher);
        }
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return stat;
        }
    }

    public List<String> getChildren(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            ArrayList<String> children = new ArrayList<String>();
            children.addAll(n.children);
            if (watcher != null) {
                childWatches.addWatch(path, watcher);
            }
            return children;
        }
    }

    public Stat setACL(String path, List<ACL> acl, int version) throws KeeperException.NoNodeException {
        Stat stat = new Stat();
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.stat.setAversion(version);
            n.acl = convertAcls(acl);
            n.copyStat(stat);
            return stat;
        }
    }

    @SuppressWarnings("unchecked")
    public List<ACL> getACL(String path, Stat stat) throws KeeperException.NoNodeException {
        DataNode n = nodes.get(path);
        if (n == null) {
            throw new KeeperException.NoNodeException();
        }
        synchronized (n) {
            n.copyStat(stat);
            return new ArrayList<ACL>(convertLong(n.acl));
        }
    }

    static public class ProcessTxnResult {

        public long clientId;

        public int cxid;

        public long zxid;

        public int err;

        public int type;

        public String path;

        public Stat stat;

        /**
         * Equality is defined as the clientId and the cxid being the same. This
         * allows us to use hash tables to track completion of transactions.
         *
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object o) {
            if (o instanceof ProcessTxnResult) {
                ProcessTxnResult other = (ProcessTxnResult) o;
                return other.clientId == clientId && other.cxid == cxid;
            }
            return false;
        }

        /**
         * See equals() to find the rational for how this hashcode is generated.
         *
         * @see ProcessTxnResult#equals(Object)
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return (int) ((clientId ^ cxid) % Integer.MAX_VALUE);
        }
    }

    public volatile long lastProcessedZxid = 0;

    @SuppressWarnings("unchecked")
    public ProcessTxnResult processTxn(TxnHeader header, Record txn) {
        ProcessTxnResult rc = new ProcessTxnResult();
        try {
            rc.clientId = header.getClientId();
            rc.cxid = header.getCxid();
            rc.zxid = header.getZxid();
            rc.type = header.getType();
            rc.err = 0;
            if (rc.zxid > lastProcessedZxid) {
                lastProcessedZxid = rc.zxid;
            }
            switch(header.getType()) {
                case OpCode.create:
                    CreateTxn createTxn = (CreateTxn) txn;
                    debug = "Create transaction for " + createTxn.getPath();
                    createNode(createTxn.getPath(), createTxn.getData(), createTxn.getAcl(), createTxn.getEphemeral() ? header.getClientId() : 0, header.getZxid(), header.getTime());
                    rc.path = createTxn.getPath();
                    break;
                case OpCode.delete:
                    DeleteTxn deleteTxn = (DeleteTxn) txn;
                    debug = "Delete transaction for " + deleteTxn.getPath();
                    deleteNode(deleteTxn.getPath(), header.getZxid());
                    break;
                case OpCode.setData:
                    SetDataTxn setDataTxn = (SetDataTxn) txn;
                    debug = "Set data for  transaction for " + setDataTxn.getPath();
                    rc.stat = setData(setDataTxn.getPath(), setDataTxn.getData(), setDataTxn.getVersion(), header.getZxid(), header.getTime());
                    break;
                case OpCode.setACL:
                    SetACLTxn setACLTxn = (SetACLTxn) txn;
                    rc.stat = setACL(setACLTxn.getPath(), setACLTxn.getAcl(), setACLTxn.getVersion());
                    break;
                case OpCode.closeSession:
                    killSession(header.getClientId(), header.getZxid());
                    break;
                case OpCode.error:
                    ErrorTxn errTxn = (ErrorTxn) txn;
                    rc.err = errTxn.getErr();
                    break;
            }
        } catch (KeeperException e) {
            // These are expected errors since we take a lazy snapshot
            if (initialized || (e.getCode() != Code.NoNode && e.getCode() != Code.NodeExists)) {
                LOG.warn(debug);
                LOG.error("FIXMSG", e);
            }
        }
        return rc;
    }

    void killSession(long session, long zxid) {
        // are again called from FinalRequestProcessor in sequence.
        HashSet<String> list = ephemerals.remove(session);
        if (list != null) {
            for (String path : list) {
                try {
                    deleteNode(path, zxid);
                    ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK, "Deleting ephemeral node " + path + " for session 0x" + Long.toHexString(session));
                } catch (KeeperException e) {
                    LOG.error("FIXMSG", e);
                }
            }
        }
    }

    /**
     * this method uses a stringbuilder to create a new
     * path for children. This is faster than string
     * appends ( str1 + str2).
     * @param oa OutputArchive to write to.
     * @param path a string builder.
     * @throws IOException
     * @throws InterruptedException
     */
    void serializeNode(OutputArchive oa, StringBuilder path) throws IOException {
        String pathString = path.toString();
        DataNode node = getNode(pathString);
        if (node == null) {
            return;
        }
        String[] children = null;
        synchronized (node) {
            scount++;
            oa.writeString(pathString, "path");
            oa.writeRecord(node, "node");
            children = node.children.toArray(new String[node.children.size()]);
        }
        path.append('/');
        int off = path.length();
        if (children != null) {
            for (String child : children) {
                // to truncate the previous bytes of string.
                path.delete(off, Integer.MAX_VALUE);
                path.append(child);
                serializeNode(oa, path);
            }
        }
    }

    int scount;

    public boolean initialized = false;

    private void deserializeList(Map<Long, List<ACL>> longKeyMap, InputArchive ia) throws IOException {
        int i = ia.readInt("map");
        while (i > 0) {
            Long val = ia.readLong("long");
            if (aclIndex < val) {
                aclIndex = val;
            }
            List<ACL> aclList = new ArrayList<ACL>();
            Index j = ia.startVector("acls");
            while (!j.done()) {
                ACL acl = new ACL();
                acl.deserialize(ia, "acl");
                aclList.add(acl);
                j.incr();
            }
            longKeyMap.put(val, aclList);
            aclKeyMap.put(aclList, val);
            i--;
        }
    }

    private synchronized void serializeList(Map<Long, List<ACL>> longKeyMap, OutputArchive oa) throws IOException {
        oa.writeInt(longKeyMap.size(), "map");
        Set<Map.Entry<Long, List<ACL>>> set = longKeyMap.entrySet();
        for (Map.Entry<Long, List<ACL>> val : set) {
            oa.writeLong(val.getKey(), "long");
            List<ACL> aclList = val.getValue();
            oa.startVector(aclList, "acls");
            for (ACL acl : aclList) {
                acl.serialize(oa, "acl");
            }
            oa.endVector(aclList, "acls");
        }
    }

    public void serialize(OutputArchive oa, String tag) throws IOException {
        scount = 0;
        serializeList(longKeyMap, oa);
        serializeNode(oa, new StringBuilder(""));
        // we need to check if clear had been called in between the snapshot.
        if (root != null) {
            oa.writeString("/", "path");
        }
    }

    public void deserialize(InputArchive ia, String tag) throws IOException {
        deserializeList(longKeyMap, ia);
        nodes.clear();
        String path = ia.readString("path");
        while (!path.equals("/")) {
            DataNode node = new DataNode();
            ia.readRecord(node, "node");
            nodes.put(path, node);
            int lastSlash = path.lastIndexOf('/');
            if (lastSlash == -1) {
                root = node;
            } else {
                String parentPath = path.substring(0, lastSlash);
                node.parent = nodes.get(parentPath);
                node.parent.children.add(path.substring(lastSlash + 1));
                long eowner = node.stat.getEphemeralOwner();
                if (eowner != 0) {
                    HashSet<String> list = ephemerals.get(eowner);
                    if (list == null) {
                        list = new HashSet<String>();
                        ephemerals.put(eowner, list);
                    }
                    list.add(path);
                }
            }
            path = ia.readString("path");
        }
        nodes.put("/", root);
    }

    public String dumpEphemerals() {
        Set<Long> keys = ephemerals.keySet();
        StringBuffer sb = new StringBuffer("Sessions with Ephemerals (" + keys.size() + "):\n");
        for (long k : keys) {
            sb.append("0x" + Long.toHexString(k));
            sb.append(":\n");
            HashSet<String> tmp = ephemerals.get(k);
            synchronized (tmp) {
                for (String path : tmp) {
                    sb.append("\t" + path + "\n");
                }
            }
        }
        return sb.toString();
    }

    public void removeCnxn(Watcher watcher) {
        dataWatches.removeWatcher(watcher);
        childWatches.removeWatcher(watcher);
    }

    public void clear() {
        root = null;
        nodes.clear();
        ephemerals.clear();
    }

    public void setWatches(long relativeZxid, List<String> dataWatches, List<String> existWatches, List<String> childWatches, Watcher watcher) {
        for (String path : dataWatches) {
            DataNode node = getNode(path);
            WatchedEvent e = null;
            if (node == null) {
                e = new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path);
            } else if (node.stat.getCzxid() > relativeZxid) {
                e = new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, path);
            } else if (node.stat.getMzxid() > relativeZxid) {
                e = new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path);
            }
            if (e != null) {
                watcher.process(e);
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : existWatches) {
            DataNode node = getNode(path);
            WatchedEvent e = null;
            if (node == null) {
            } else if (node.stat.getMzxid() > relativeZxid) {
                e = new WatchedEvent(EventType.NodeDataChanged, KeeperState.SyncConnected, path);
            } else {
                e = new WatchedEvent(EventType.NodeCreated, KeeperState.SyncConnected, path);
            }
            if (e != null) {
                watcher.process(e);
            } else {
                this.dataWatches.addWatch(path, watcher);
            }
        }
        for (String path : childWatches) {
            DataNode node = getNode(path);
            WatchedEvent e = null;
            if (node == null) {
                e = new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, path);
            } else if (node.stat.getPzxid() > relativeZxid) {
                e = new WatchedEvent(EventType.NodeChildrenChanged, KeeperState.SyncConnected, path);
            }
            if (e != null) {
                watcher.process(e);
            } else {
                this.childWatches.addWatch(path, watcher);
            }
        }
    }
}
