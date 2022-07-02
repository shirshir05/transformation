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
package org.apache.zookeeper.server.auth;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.ServerCnxn;

public class SASLAuthenticationProvider implements AuthenticationProvider {

    public String getScheme() {
        return "sasl";
    }

    public KeeperException.Code handleAuthentication(ServerCnxn cnxn, byte[] authData) {
        // a call to this method (SASLAuthenticationProvider:handleAuthentication()) at session initiation.
        return KeeperException.Code.AUTHFAILED;
    }

    public boolean matches(String id, String aclExpr) {
        if (System.getProperty("zookeeper.superUser") != null) {
            if (id.equals(System.getProperty("zookeeper.superUser")) || id.equals(aclExpr)) {
                return true;
            }
        }
        if ((id.equals("super") || id.equals(aclExpr))) {
            return true;
        }
        String readAccessUser = System.getProperty("zookeeper.readUser");
        if (null != readAccessUser && aclExpr.equals(readAccessUser)) {
            return true;
        }
        return false;
    }

    public boolean isAuthenticated() {
        return true;
    }

    public boolean isValid(String id) {
        // 
        try {
            new KerberosName(id);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
