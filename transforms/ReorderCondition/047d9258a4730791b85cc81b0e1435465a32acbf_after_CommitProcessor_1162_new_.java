/*
 * Copyright 2008, Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.zookeeper.server.quorum;

import java.util.ArrayList;
import java.util.LinkedList;
import com.yahoo.zookeeper.ZooDefs.OpCode;
import com.yahoo.zookeeper.server.Request;
import com.yahoo.zookeeper.server.RequestProcessor;
import com.yahoo.zookeeper.server.ZooLog;

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up.
 */
public class CommitProcessor extends Thread implements RequestProcessor {

    /**
     * Requests that we are holding until the commit comes in.
     */
    LinkedList<Request> queuedRequests = new LinkedList<Request>();

    /**
     * Requests that have been committed.
     */
    LinkedList<Request> committedRequests = new LinkedList<Request>();

    RequestProcessor nextProcessor;

    public CommitProcessor(RequestProcessor nextProcessor) {
        this.nextProcessor = nextProcessor;
        start();
    }

    boolean finished = false;

    public void run() {
        try {
            Request nextPending = null;
            ArrayList<Request> toProcess = new ArrayList<Request>();
            while (!finished) {
                int len = toProcess.size();
                for (int i = 0; len > i; i++) {
                    nextProcessor.processRequest(toProcess.get(i));
                }
                toProcess.clear();
                synchronized (this) {
                    if ((queuedRequests.size() == 0 || nextPending != null) && committedRequests.size() == 0) {
                        wait();
                        continue;
                    }
                    // request
                    if ((queuedRequests.size() == 0 || nextPending != null) && committedRequests.size() > 0) {
                        Request r = committedRequests.remove();
                        /*
                         * We match with nextPending so that we can move to the
                         * next request when it is committed. We also want to
                         * use nextPending because it has the cnxn member set
                         * properly.
                         */
                        if (nextPending != null && nextPending.sessionId == r.sessionId && nextPending.cxid == r.cxid) {
                            // the pointer to the connection in the request
                            nextPending.hdr = r.hdr;
                            nextPending.txn = r.txn;
                            nextPending.zxid = r.zxid;
                            toProcess.add(nextPending);
                            nextPending = null;
                        } else {
                            // send the commit packet
                            toProcess.add(r);
                        }
                    }
                }
                // waiting
                if (nextPending != null) {
                    continue;
                }
                synchronized (this) {
                    // Process the next requests in the queuedRequests
                    while (nextPending == null && queuedRequests.size() > 0) {
                        Request request = queuedRequests.remove();
                        switch(request.type) {
                            case OpCode.create:
                            case OpCode.delete:
                            case OpCode.setData:
                            case OpCode.setACL:
                            case OpCode.createSession:
                            case OpCode.closeSession:
                                nextPending = request;
                                break;
                            default:
                                toProcess.add(request);
                        }
                    }
                }
            }
        } catch (Exception e) {
            ZooLog.logException(e);
        }
        ZooLog.logTextTraceMessage("CommitProcessor exited loop!", ZooLog.textTraceMask);
    }

    synchronized public void commit(Request request) {
        if (!finished) {
            if (request == null) {
                ZooLog.logException(new Exception("committing a null! "));
                return;
            }
            committedRequests.add(request);
            notifyAll();
        }
    }

    synchronized public void processRequest(Request request) {
        // request.cnxn);
        if (!finished) {
            queuedRequests.add(request);
            notifyAll();
        }
    }

    public void shutdown() {
        finished = true;
        queuedRequests.clear();
        synchronized (this) {
            notifyAll();
        }
        nextProcessor.shutdown();
    }
}
