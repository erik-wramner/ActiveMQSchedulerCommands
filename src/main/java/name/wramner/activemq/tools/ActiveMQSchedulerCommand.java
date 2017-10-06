/*
 * Copyright 2016 Erik Wramner.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package name.wramner.activemq.tools;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import name.wramner.activemq.tools.ActiveMQSchedulerCommand.Config;

/**
 * Base class for ActiveMQ scheduler command tools.
 *
 * @author Erik Wramner
 * @param <T> the configuration class.
 */
public abstract class ActiveMQSchedulerCommand<T extends Config> {

    /**
     * Run command.
     *
     * @param args The command line.
     * @param config The configuration instance.
     */
    protected void run(String[] args, T config) {
        if (parseCommandLine(args, config)) {
            Connection conn = null;
            Session session = null;
            try {
                conn = openConnection(config.getBrokerUrl(), config.getUserName(), config.getPassword());
                session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                conn.start();

                Destination schedulerManagementDestination = session
                                .createTopic(ScheduledMessage.AMQ_SCHEDULER_MANAGEMENT_DESTINATION);

                processCommand(config, session, schedulerManagementDestination);
            } catch (JMSException e) {
                System.err.println("Failed!");
                e.printStackTrace(System.err);
            } finally {
                closeSafely(conn, session);
            }
        }
    }

    /**
     * Process command.
     *
     * @param config The configuration.
     * @param session The session.
     * @param schedulerManagementDestination The destination for the scheduler.
     * @throws JMSException on errors.
     */
    protected abstract void processCommand(T config, Session session, Destination schedulerManagementDestination)
                    throws JMSException;

    /**
     * Close session and connection ignoring errors.
     *
     * @param conn The connection.
     * @param session The session.
     */
    private void closeSafely(Connection conn, Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException e) {
            }
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (JMSException e) {
            }
        }
    }

    /**
     * Send a command message to the scheduler.
     *
     * @param session The session.
     * @param schedulerManagementDestination The destination.
     * @param schedulerCommandMessage The message.
     * @throws JMSException on errors.
     */
    protected void sendCommandToScheduler(Session session, Destination schedulerManagementDestination,
                    Message schedulerCommandMessage) throws JMSException {
        MessageProducer producer = session.createProducer(schedulerManagementDestination);
        try {
            producer.send(schedulerCommandMessage);
        } finally {
            producer.close();
        }
    }

    /**
     * Open connection to broker.
     *
     * @param url The url.
     * @param username The user name.
     * @param password The password.
     * @return connection.
     * @throws JMSException on errors.
     */
    private Connection openConnection(String url, String username, String password) throws JMSException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(username, password, url);
        // Handle arbitrary object messages
        factory.setTrustAllPackages(true);
        return factory.createConnection();
    }

    /**
     * Parse the command line into the specified configuration.
     *
     * @param args The command line.
     * @param config The configuration class.
     * @return true if successful, false on errors such as missing arguments.
     */
    protected boolean parseCommandLine(String[] args, T config) {
        CmdLineParser parser = new CmdLineParser(config);
        try {
            parser.parseArgument(args);
            config.verify();
            return true;
        } catch (CmdLineException | ConfigurationException e) {
            System.out.println("Error: " + e.getMessage());
            System.out.println("The supported options are:");
            parser.printUsage(System.out);
            System.out.println();
            return false;
        }
    }

    /**
     * Exception thrown if there are configuration errors.
     */
    public static class ConfigurationException extends Exception {
        private static final long serialVersionUID = 1L;

        public ConfigurationException(String message) {
            super(message);
        }
    }

    /**
     * Basic configuration options.
     */
    public static class Config {
        @Option(name = "-url", aliases = { "--broker-url" }, usage = "ActiveMQ broker URL", required = true)
        private String _brokerUrl;

        @Option(name = "-user", aliases = { "--broker-user" }, usage = "ActiveMQ user name if using authentication")
        private String _userName;

        @Option(name = "-pw", aliases = {
                        "--broker-password" }, usage = "ActiveMQ password if using authentication", depends = {
                                        "-user" })
        private String _password;

        protected void verify() throws ConfigurationException {
            // Check that the configuration makes sense
        }

        public String getBrokerUrl() {
            return _brokerUrl;
        }

        public String getUserName() {
            return _userName;
        }

        public String getPassword() {
            return _password;
        }
    }
}
