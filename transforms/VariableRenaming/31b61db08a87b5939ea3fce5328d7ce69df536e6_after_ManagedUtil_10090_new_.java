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
package org.apache.zookeeper.jmx;

import java.util.Enumeration;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared utilities
 */
public class ManagedUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ManagedUtil.class);

    private static final boolean isLog4jJmxEnabled() {
        boolean var0 = false;
        try {
            Class.forName("org.apache.log4j.spi.LoggerRepository");
            if (Boolean.getBoolean("zookeeper.jmx.log4j.disable") == true) {
                LOG.info("Log4j found but jmx support is disabled.");
            } else {
                var0 = true;
                LOG.info("Log4j found with jmx enabled.");
            }
        } catch (ClassNotFoundException e) {
            LOG.info("Log4j not found.");
        }
        return var0;
    }

    /**
     * Register the log4j JMX mbeans. Set environment variable
     * "zookeeper.jmx.log4j.disable" to true to disable registration.
     * @see http://logging.apache.org/log4j/1.2/apidocs/index.html?org/apache/log4j/jmx/package-summary.html
     * @throws JMException if registration fails
     */
    @SuppressWarnings("rawtypes")
    public static void registerLog4jMBeans() throws JMException {
        if (isLog4jJmxEnabled()) {
            LOG.debug("registerLog4jMBeans()");
            MBeanServer mbs = MBeanRegistry.getInstance().getPlatformMBeanServer();
            try {
                // org.apache.log4j.jmx.HierarchyDynamicMBean hdm = new org.apache.log4j.jmx.HierarchyDynamicMBean();
                Object hdm = Class.forName("org.apache.log4j.jmx.HierarchyDynamicMBean").newInstance();
                ObjectName mbo = new ObjectName("log4j:hiearchy=default");
                mbs.registerMBean(hdm, mbo);
                // org.apache.log4j.Logger.getRootLogger();
                Object rootLogger = Class.forName("org.apache.log4j.Logger").getMethod("getRootLogger", (Class<?>[]) null).invoke(null, (Object[]) null);
                // hdm.addLoggerMBean(rootLogger.getName());
                Object rootLoggerName = rootLogger.getClass().getMethod("getName", (Class<?>[]) null).invoke(rootLogger, (Object[]) null);
                hdm.getClass().getMethod("addLoggerMBean", String.class).invoke(hdm, rootLoggerName);
                // org.apache.log4j.LogManager.getLoggerRepository();
                Object r = Class.forName("org.apache.log4j.LogManager").getMethod("getLoggerRepository", (Class<?>[]) null).invoke(null, (Object[]) null);
                // Enumeration enumer = r.getCurrentLoggers();
                Enumeration enumer = (Enumeration) r.getClass().getMethod("getCurrentLoggers", (Class<?>[]) null).invoke(r, (Object[]) null);
                while (enumer.hasMoreElements()) {
                    Object logger = enumer.nextElement();
                    // hdm.addLoggerMBean(logger.getName());
                    Object loggerName = logger.getClass().getMethod("getName", (Class<?>[]) null).invoke(logger, (Object[]) null);
                    hdm.getClass().getMethod("addLoggerMBean", String.class).invoke(hdm, loggerName);
                }
            } catch (Exception e) {
                LOG.error("Problems while registering log4j jmx beans!", e);
                throw new JMException(e.toString());
            }
        }
    }
}
