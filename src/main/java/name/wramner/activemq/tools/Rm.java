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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import org.apache.activemq.ScheduledMessage;
import org.kohsuke.args4j.Option;

import name.wramner.activemq.tools.Rm.RmConfig;

/**
 * Command that removes messages from the ActiveMQ scheduler, either all messages or a specific one.
 * <p>
 * The way it works is that a message is sent to the scheduler with a command to remove all or a specific message
 * identified by id. The scheduler hopefully obeys. There is no response to indicate success or failure.
 * <p>
 * Note that the scheduler is local to a specific broker, so &quote;remove all&quot; means remove all messages scheduled
 * on that broker. In a cluster this may be a bit confusing, but that is how it works.
 *
 * @author Erik Wramner
 */
public class Rm extends ActiveMQSchedulerCommand<RmConfig> {

    /**
     * Program entry point.
     *
     * @param args The command line.
     */
    public static void main(String[] args) {
        new Rm().run(args, new RmConfig());
    }

    /**
     * Process command, send a remove or remove all message to the scheduler.
     *
     * @param config The rm configuration.
     * @param session The session.
     * @param schedulerManagementDestination The destination to the scheduler.
     * @throws JMSException on errors.
     */
    @Override
    public void processCommand(RmConfig config, Session session, Destination schedulerManagementDestination)
                    throws JMSException {
        Message schedulerCommandMessage = session.createMessage();

        if (config.shouldRemoveAll()) {
            schedulerCommandMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION,
                            ScheduledMessage.AMQ_SCHEDULER_ACTION_REMOVEALL);
        } else {
            schedulerCommandMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULER_ACTION,
                            ScheduledMessage.AMQ_SCHEDULER_ACTION_REMOVE);
            schedulerCommandMessage.setStringProperty(ScheduledMessage.AMQ_SCHEDULED_ID, config.getId());
        }

        sendCommandToScheduler(session, schedulerManagementDestination, schedulerCommandMessage);
    }

    /**
     * Configuration specific to the rm command.
     */
    static class RmConfig extends ActiveMQSchedulerCommand.Config {
        @Option(name = "-a", aliases = { "--all" }, usage = "Remove ALL scheduled messages", forbids = "-i")
        private boolean _all;

        @Option(name = "-i", aliases = { "--id" }, usage = "Remove specific scheduled message", forbids = "-a")
        private String _id;

        public boolean shouldRemoveAll() {
            return _all;
        }

        public String getId() {
            return _id;
        }

        @Override
        protected void verify() throws ConfigurationException {
            super.verify();
            if (!shouldRemoveAll() && getId() == null) {
                throw new ConfigurationException("Please specify scheduler job id (-i id) or all (-a)!");
            }
        }
    }
}
