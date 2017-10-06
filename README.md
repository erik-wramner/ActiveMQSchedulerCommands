# ActiveMQSchedulerCommands

This repository contains commands for interacting with the ActiveMQ scheduler: ls and rm.
As the names imply they list and remove scheduled messages.

A key feature of the ls command is that it can show the destinations for scheduled messages.
Normally the scheduler is somewhat of a black box, there is no easy way to see which queue
or topic that has a large number of scheduled messages.

Warning! The ls command actually consumes copies of all the scheduled messages on a
temporary destination. Think twice before using it if there are millions of scheduled
messages pending. That is an unlikely situation as the scheduler seems to run into
performance issues much earlier, though.
