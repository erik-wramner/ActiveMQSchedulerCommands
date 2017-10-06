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

import java.util.Enumeration;
import java.util.Map;
import java.util.TreeMap;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.command.ActiveMQMessage;
import org.kohsuke.args4j.Option;

import name.wramner.activemq.tools.List.ListConfig;
import name.wramner.util.HexDumpEncoder;

/**
 * Command that lists scheduled messages for an ActiveMQ broker.
 * <p>
 * Note that the original destination will only be available starting with ActiveMQ 5.15.1.
 * <p>
 * Also note that the scheduler is local to a given broker, so in a cluster it may be necessary to contact each broker
 * in turn in order to get the complete view.
 *
 * @author Erik Wramner
 */
public class List extends ActiveMQSchedulerCommand<ListConfig> {
    private static final int JMS_INITIAL_RECEIVE_TIMEOUT = 5000;
    private static final int JMS_RECEIVE_TIMEOUT = 200;

    /**
     * Program entry point.
     *
     * @param args The command line.
     */
    public static void main(String[] args) {
        new List().run(args, new ListConfig());
    }

    /**
     * Process command, send a browse request to the broker and receive scheduled messages on a temporary destination.
     * Print each message with properties and content or just the totals.
     *
     * @param config The list configuration.
     * @param session The session.
     * @param schedulerManagementDestination The destination to the scheduler.
     * @throws JMSException on errors.
     */
    @Override
    public void processCommand(ListConfig config, Session session, Destination schedulerManagementDestination)
                    throws JMSException {
        Destination schedulerResponseDestination = session.createTemporaryQueue();
        MessageConsumer responseConsumer = session.createConsumer(schedulerResponseDestination);
        try {

            Message schedulerCommandMessage = session.createMessage();
            schedulerCommandMessage.setJMSReplyTo(schedulerResponseDestination);
            schedulerCommandMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION,
                            ScheduledMessage.AMQ_SCHEDULER_ACTION_BROWSE);

            sendCommandToScheduler(session, schedulerManagementDestination, schedulerCommandMessage);

            Map<String, Integer> queueToTotalsMap = new TreeMap<>();
            for (Message m = responseConsumer.receive(JMS_INITIAL_RECEIVE_TIMEOUT); m != null; m = responseConsumer
                            .receive(JMS_RECEIVE_TIMEOUT)) {
                ActiveMQMessage schedulerResponseMessage = (ActiveMQMessage) m;
                String queue = schedulerResponseMessage.getOriginalDestination() != null
                                ? schedulerResponseMessage.getOriginalDestination().toString() : "<unknown>";
                if (config.isShowTotalsEnabled()) {
                    Integer count = queueToTotalsMap.get(queue);
                    count = count != null ? Integer.valueOf(count.intValue() + 1) : Integer.valueOf(1);
                    queueToTotalsMap.put(queue, count);
                } else {
                    System.out.println(String.format("%s\t%s",
                                    schedulerResponseMessage.getStringProperty(ScheduledMessage.AMQ_SCHEDULED_ID),
                                    queue));

                    if (config.isShowPropertiesEnabled()) {
                        System.out.println("-- Properties");
                        @SuppressWarnings("rawtypes")
                        Enumeration propertyNames = schedulerResponseMessage.getPropertyNames();
                        while (propertyNames.hasMoreElements()) {
                            String name = propertyNames.nextElement().toString();
                            System.out.println(name + " = " + schedulerResponseMessage.getStringProperty(name));
                        }
                    }
                    if (config.isShowContentEnabled()) {
                        System.out.println("-- Content");
                        System.out.println(
                                        new HexDumpEncoder().encode(schedulerResponseMessage.getContent().getData()));
                    }
                    if (config.isShowPropertiesEnabled() || config.isShowContentEnabled()) {
                        System.out.println("--");
                        System.out.println();
                    }
                }
            }

            if (config.isShowTotalsEnabled()) {
                int longestQueueName = queueToTotalsMap.keySet().stream().mapToInt(String::length).max().orElse(0);
                queueToTotalsMap.entrySet().stream().forEachOrdered(entry -> System.out.println(
                                String.format("%-" + longestQueueName + "s %10d", entry.getKey(), entry.getValue())));
            }
        } finally {
            responseConsumer.close();
        }
    }

    /**
     * Extended configuration for the list command.
     */
    static class ListConfig extends ActiveMQSchedulerCommand.Config {
        @Option(name = "-c", aliases = { "--show-content" }, usage = "Show message content")
        private boolean _showContent;

        @Option(name = "-p", aliases = { "--show-properties" }, usage = "Show message properties")
        private boolean _showProperties;

        @Option(name = "-t", aliases = { "--totals-per-queue" }, usage = "Show totals per queue", forbids = { "-c",
                        "-p" })
        private boolean _showTotals;

        public boolean isShowContentEnabled() {
            return _showContent && !_showTotals;
        }

        public boolean isShowPropertiesEnabled() {
            return _showProperties && !_showTotals;
        }

        public boolean isShowTotalsEnabled() {
            return _showTotals;
        }
    }
}
