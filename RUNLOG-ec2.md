# Run log

One entry per experiment, appended as soon as the run finishes. Each entry
names the case, what it tests, and the outcome. The full detail for a run is
in the commented header of its timeline file.

Two files per run, both in runs/:
  <run_id>.timeline.csv      readable header, then one row per second
  <run_id>.messages.csv.gz   raw, one row per message
The files the individual processes wrote are kept in work/<run_id>/, so the
two joins above can be checked against their inputs.

Terms: 'useful loss' is a message the broker removed while it could still
have been processed in time. 'too late' is a message that was processed after
its deadline had already passed, i.e. wasted work. 'neighbour' is a second,
healthy queue on the same broker, unrelated to the slow one.
--------------------------------------------------------------------------------------
RUN 1  R-C1-unbounded_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 450248 in time, 37423 too late, 42148 lost while useful, 47170 lost after deadline, 0 refused, 83011 never sent
alarm     9 episodes, 33% of the slow phase, 2 purge(s)
neighbour confirm p99 0.81 -> 9806.47 ms, 19233 publishes lost
drain     45 s measured vs 49 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r1.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 2  R-C2-maxlen-drophead_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467683 in time, 0 too late, 192317 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.8 -> 0.81 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r1.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 3  R-C3-reject-publish_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363351 in time, 104281 too late, 0 lost while useful, 0 lost after deadline, 192368 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.8 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r1.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 4  R-C4-ttl_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 485506 in time, 0 too late, 123122 lost while useful, 0 lost after deadline, 0 refused, 51372 never sent
alarm     7 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.79 -> 9564.22 ms, 9626 publishes lost
drain     43 s measured vs 48 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r1.timeline.csv, runs/R-C4-ttl_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 5  R-C5-combined_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467654 in time, 0 too late, 12496 lost while useful, 0 lost after deadline, 179850 refused, 0 never sent
alarm     none
neighbour confirm p99 0.79 -> 0.8 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r1.timeline.csv, runs/R-C5-combined_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 6  R-C9-wide-headroom_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 421077 in time, 98330 too late, 18630 lost while useful, 74771 lost after deadline, 0 refused, 47192 never sent
alarm     6 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.79 -> 9490.6 ms, 9627 publishes lost
drain     78 s measured vs 76 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r1.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 7  K-C1-long-retention_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 235771 in time, 424229 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.61 -> 5.94 ms, 0 publishes lost
drain     225 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r1.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 8  K-C2-short-retention_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 332054 in time, 234363 too late, 0 lost while useful, 93583 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.51 -> 5.91 ms, 0 publishes lost
drain     128 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r1.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 9  K-C4-consumer-deadline_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 473547 in time, 9591 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.45 -> 5.88 ms, 0 publishes lost
drain     62 s measured vs 62 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r1.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 10  K-C6-retention-below-D_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 477408 in time, 0 too late, 182592 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.67 -> 5.89 ms, 0 publishes lost
drain     37 s measured vs 54 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r1.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 11  R-C11-reject-correct_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 467345 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 192655 refused, 0 never sent
alarm     none
neighbour confirm p99 0.74 -> 0.77 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r1.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 12  R-C12-maxlen-correct-wide_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399302 in time, 104091 too late, 156607 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.75 -> 0.77 ms, 0 publishes lost
drain     62 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r1.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 13  R-C1-unbounded_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 231857 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 8143 never sent
alarm     1 episodes, 10% of the slow phase, 0 purge(s)
neighbour confirm p99 0.77 -> 9180.42 ms, 1486 publishes lost
drain     48 s measured vs 45 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r1.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 14  R-C2-maxlen-drophead_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 210020 in time, 0 too late, 29980 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.78 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r1.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 15  R-C3-reject-publish_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 210027 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 29973 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.79 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r1.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 16  R-C1-unbounded_f10_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 353236 in time, 28199 too late, 33416 lost while useful, 108960 lost after deadline, 0 refused, 136189 never sent
alarm     6 episodes, 50% of the slow phase, 3 purge(s)
neighbour confirm p99 0.77 -> 19720.1 ms, 31087 publishes lost
drain     11 s measured vs 10 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f10_s360_A_r1.timeline.csv, runs/R-C1-unbounded_f10_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 17  R-C1-unbounded_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 451509 in time, 37925 too late, 39980 lost while useful, 47757 lost after deadline, 0 refused, 82829 never sent
alarm     9 episodes, 33% of the slow phase, 2 purge(s)
neighbour confirm p99 0.77 -> 9788.25 ms, 19104 publishes lost
drain     48 s measured vs 46 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r2.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 18  R-C2-maxlen-drophead_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467667 in time, 0 too late, 192333 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.77 -> 0.78 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r2.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 19  R-C3-reject-publish_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363325 in time, 104300 too late, 0 lost while useful, 0 lost after deadline, 192375 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.77 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r2.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 20  R-C4-ttl_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 462686 in time, 0 too late, 148554 lost while useful, 0 lost after deadline, 0 refused, 48760 never sent
alarm     6 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.74 -> 9488.83 ms, 9607 publishes lost
drain     20 s measured vs 19 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r2.timeline.csv, runs/R-C4-ttl_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 21  R-C5-combined_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467653 in time, 0 too late, 12481 lost while useful, 0 lost after deadline, 179866 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.77 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r2.timeline.csv, runs/R-C5-combined_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 22  R-C9-wide-headroom_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 418650 in time, 102485 too late, 20601 lost while useful, 73654 lost after deadline, 0 refused, 44610 never sent
alarm     6 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.75 -> 9488.23 ms, 9202 publishes lost
drain     80 s measured vs 78 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r2.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 23  K-C1-long-retention_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 235767 in time, 424233 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.41 -> 5.85 ms, 0 publishes lost
drain     225 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r2.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 24  K-C2-short-retention_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 332160 in time, 234239 too late, 0 lost while useful, 93601 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.35 -> 5.85 ms, 0 publishes lost
drain     129 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r2.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 25  K-C4-consumer-deadline_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 472758 in time, 10219 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.38 -> 5.84 ms, 0 publishes lost
drain     62 s measured vs 62 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r2.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 26  K-C6-retention-below-D_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 477412 in time, 0 too late, 182588 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.38 -> 5.85 ms, 0 publishes lost
drain     37 s measured vs 54 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r2.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 27  R-C11-reject-correct_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 467348 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 192652 refused, 0 never sent
alarm     none
neighbour confirm p99 0.75 -> 0.76 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r2.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 28  R-C12-maxlen-correct-wide_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399319 in time, 104087 too late, 156594 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.74 -> 0.75 ms, 0 publishes lost
drain     62 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r2.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 29  R-C1-unbounded_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 231276 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 8724 never sent
alarm     1 episodes, 10% of the slow phase, 0 purge(s)
neighbour confirm p99 0.78 -> 9180.0 ms, 1496 publishes lost
drain     47 s measured vs 45 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r2.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 30  R-C2-maxlen-drophead_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 210020 in time, 0 too late, 29980 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.75 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r2.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 31  R-C3-reject-publish_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 210018 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 29982 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.79 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r2.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 32  R-C1-unbounded_f10_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 353040 in time, 28443 too late, 44335 lost while useful, 108886 lost after deadline, 0 refused, 125296 never sent
alarm     6 episodes, 50% of the slow phase, 3 purge(s)
neighbour confirm p99 0.76 -> 19493.15 ms, 30386 publishes lost
drain     10 s measured vs 10 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f10_s360_A_r2.timeline.csv, runs/R-C1-unbounded_f10_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 33  R-C1-unbounded_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 450640 in time, 37271 too late, 44645 lost while useful, 46780 lost after deadline, 0 refused, 80664 never sent
alarm     9 episodes, 34% of the slow phase, 2 purge(s)
neighbour confirm p99 0.75 -> 9805.07 ms, 19088 publishes lost
drain     46 s measured vs 46 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r3.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 34  R-C2-maxlen-drophead_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467674 in time, 0 too late, 192326 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.78 -> 0.77 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r3.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 35  R-C3-reject-publish_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363355 in time, 104300 too late, 0 lost while useful, 0 lost after deadline, 192345 refused, 0 never sent
alarm     none
neighbour confirm p99 0.78 -> 0.76 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r3.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 36  R-C4-ttl_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 484207 in time, 0 too late, 122684 lost while useful, 0 lost after deadline, 0 refused, 53109 never sent
alarm     7 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.75 -> 9495.73 ms, 9612 publishes lost
drain     42 s measured vs 48 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r3.timeline.csv, runs/R-C4-ttl_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 37  R-C5-combined_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467684 in time, 0 too late, 12500 lost while useful, 0 lost after deadline, 179816 refused, 0 never sent
alarm     none
neighbour confirm p99 0.75 -> 0.78 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r3.timeline.csv, runs/R-C5-combined_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 38  R-C9-wide-headroom_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 417191 in time, 105268 too late, 22617 lost while useful, 72477 lost after deadline, 0 refused, 42447 never sent
alarm     5 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 0.74 -> 9488.03 ms, 9621 publishes lost
drain     81 s measured vs 79 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r3.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 39  K-C1-long-retention_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 235868 in time, 424132 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.38 -> 5.86 ms, 0 publishes lost
drain     225 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r3.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 40  K-C2-short-retention_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 331933 in time, 234443 too late, 0 lost while useful, 93624 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.38 -> 5.87 ms, 0 publishes lost
drain     129 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r3.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 41  K-C4-consumer-deadline_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 473153 in time, 9897 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.42 -> 5.87 ms, 0 publishes lost
drain     62 s measured vs 62 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r3.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 42  K-C6-retention-below-D_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 477412 in time, 0 too late, 182588 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 6.36 -> 5.86 ms, 0 publishes lost
drain     37 s measured vs 54 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r3.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 43  R-C11-reject-correct_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 467339 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 192661 refused, 0 never sent
alarm     none
neighbour confirm p99 0.75 -> 0.78 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r3.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 44  R-C12-maxlen-correct-wide_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399318 in time, 104100 too late, 156582 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.78 -> 0.77 ms, 0 publishes lost
drain     62 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r3.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 45  R-C1-unbounded_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 231718 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 8282 never sent
alarm     1 episodes, 10% of the slow phase, 0 purge(s)
neighbour confirm p99 0.78 -> 9181.65 ms, 1489 publishes lost
drain     48 s measured vs 45 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r3.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 46  R-C2-maxlen-drophead_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 210023 in time, 0 too late, 29977 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 0.77 -> 0.79 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r3.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 47  R-C3-reject-publish_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 210016 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 29984 refused, 0 never sent
alarm     none
neighbour confirm p99 0.76 -> 0.8 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r3.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 48  R-C1-unbounded_f10_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 348638 in time, 28548 too late, 37681 lost while useful, 107380 lost after deadline, 0 refused, 137753 never sent
alarm     9 episodes, 50% of the slow phase, 3 purge(s)
neighbour confirm p99 0.78 -> 19590.0 ms, 29889 publishes lost
drain     6 s measured vs 5 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f10_s360_A_r3.timeline.csv, runs/R-C1-unbounded_f10_s360_A_r3.messages.csv.gz
