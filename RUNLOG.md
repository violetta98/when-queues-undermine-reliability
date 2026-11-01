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
RUN 1  K-C1-long-retention_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 231735 in time, 428265 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 8.3 -> 7.58 ms, 0 publishes lost
drain     231 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r1.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 2  K-C1-long-retention_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 233042 in time, 426958 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.23 -> 6.9 ms, 0 publishes lost
drain     229 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r2.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 3  K-C1-long-retention_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C1-long-retention - Retention far longer than the run, which is how a default Kafka topic behaves during an hour-long incident.
tests     H2: does a lagging consumer harm an unrelated topic on the same broker
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB
outcome   660000 published: 232787 in time, 427213 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.26 -> 6.97 ms, 0 publishes lost
drain     230 s measured vs 218 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C1-long-retention_f5_s360_A_r3.timeline.csv, runs/K-C1-long-retention_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 4  K-C2-short-retention_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 311727 in time, 271944 too late, 0 lost while useful, 76329 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.88 -> 7.24 ms, 0 publishes lost
drain     151 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r1.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 5  K-C2-short-retention_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 312824 in time, 270846 too late, 0 lost while useful, 76330 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.19 -> 6.91 ms, 0 publishes lost
drain     149 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r2.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 6  K-C2-short-retention_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C2-short-retention - Retention of 120 s, longer than the 60 s useful lifetime.
tests     model threshold 2: deletion happens once the backlog outlives retention
broker    Kafka 4.3.1
bound     retention 120 s; segment roll 30 s; segment size 32 MiB
outcome   660000 published: 312824 in time, 270853 too late, 0 lost while useful, 76323 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 12.35 -> 6.9 ms, 0 publishes lost
drain     150 s measured vs 143 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C2-short-retention_f5_s360_A_r3.timeline.csv, runs/K-C2-short-retention_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 7  K-C3-lag-backpressure_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C3-lag-backpressure - Producer-side refusal above a lag threshold.
tests     H2/H3: producer-side refusal as a substitute for a broker bound
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; producer refuses to publish above 24000 messages of consumer lag
outcome   660000 published: 409678 in time, 55731 too late, 0 lost while useful, 0 lost after deadline, 194591 refused, 0 never sent
alarm     none
neighbour confirm p99 7.65 -> 7.16 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C3-lag-backpressure_f5_s360_A_r1.timeline.csv, runs/K-C3-lag-backpressure_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 8  K-C3-lag-backpressure_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C3-lag-backpressure - Producer-side refusal above a lag threshold.
tests     H2/H3: producer-side refusal as a substitute for a broker bound
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; producer refuses to publish above 24000 messages of consumer lag
outcome   660000 published: 409555 in time, 55587 too late, 0 lost while useful, 0 lost after deadline, 194858 refused, 0 never sent
alarm     none
neighbour confirm p99 7.2 -> 7.03 ms, 0 publishes lost
drain     25 s measured vs 23 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C3-lag-backpressure_f5_s360_A_r2.timeline.csv, runs/K-C3-lag-backpressure_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 9  K-C3-lag-backpressure_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C3-lag-backpressure - Producer-side refusal above a lag threshold.
tests     H2/H3: producer-side refusal as a substitute for a broker bound
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; producer refuses to publish above 24000 messages of consumer lag
outcome   660000 published: 409718 in time, 55228 too late, 0 lost while useful, 0 lost after deadline, 195054 refused, 0 never sent
alarm     none
neighbour confirm p99 7.16 -> 7.1 ms, 0 publishes lost
drain     26 s measured vs 23 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C3-lag-backpressure_f5_s360_A_r3.timeline.csv, runs/K-C3-lag-backpressure_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 10  K-C4-consumer-deadline_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 472340 in time, 10013 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.13 -> 6.91 ms, 0 publishes lost
drain     63 s measured vs 61 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r1.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 11  K-C4-consumer-deadline_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 472179 in time, 9891 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.29 -> 6.9 ms, 0 publishes lost
drain     64 s measured vs 61 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r2.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 12  K-C4-consumer-deadline_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C4-consumer-deadline - The consumer discards records already past their deadline instead of processing them.
tests     H1: an age check at the consumer removes the wasted work
broker    Kafka 4.3.1
bound     retention 604800 s; segment size 64 MiB; consumer discards messages already past their deadline
outcome   660000 published: 472792 in time, 9694 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.42 -> 6.95 ms, 0 publishes lost
drain     65 s measured vs 61 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C4-consumer-deadline_f5_s360_A_r3.timeline.csv, runs/K-C4-consumer-deadline_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 13  K-C6-retention-below-D_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 484155 in time, 208 too late, 175637 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.19 -> 6.95 ms, 0 publishes lost
drain     45 s measured vs 52 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r1.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 14  K-C6-retention-below-D_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 484162 in time, 189 too late, 175649 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.18 -> 6.93 ms, 0 publishes lost
drain     46 s measured vs 48 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r2.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 15  K-C6-retention-below-D_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C6-retention-below-D - Retention of 30 s, shorter than the 60 s useful lifetime.
tests     model threshold 2: silent loss becomes useful loss when R < D
broker    Kafka 4.3.1
bound     retention 30 s; segment roll 15 s; segment size 16 MiB
outcome   660000 published: 484128 in time, 247 too late, 175625 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.24 -> 6.91 ms, 0 publishes lost
drain     46 s measured vs 48 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C6-retention-below-D_f5_s360_A_r3.timeline.csv, runs/K-C6-retention-below-D_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 16  K-C7-retention-bytes_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      K-C7-retention-bytes - A byte bound instead of a time bound: retention.
tests     F3: does the size of a bound, or the broker enforcing it, decide how predictable the moment of removal is
broker    Kafka 4.3.1
bound     retention 4 MiB per partition; retention 604800 s; segment size 1 MiB
outcome   660000 published: 469912 in time, 0 too late, 190088 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 7.68 -> 7.02 ms, 0 publishes lost
drain     31 s measured vs 36 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C7-retention-bytes_f5_s360_A_r1.timeline.csv, runs/K-C7-retention-bytes_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 17  K-C7-retention-bytes_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      K-C7-retention-bytes - A byte bound instead of a time bound: retention.
tests     F3: does the size of a bound, or the broker enforcing it, decide how predictable the moment of removal is
broker    Kafka 4.3.1
bound     retention 4 MiB per partition; retention 604800 s; segment size 1 MiB
outcome   660000 published: 470070 in time, 0 too late, 189768 lost while useful, 0 lost after deadline, 0 refused, 162 never sent
alarm     none
neighbour confirm p99 9.65 -> 34.07 ms, 0 publishes lost
drain     33 s measured vs 35 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C7-retention-bytes_f5_s360_A_r2.timeline.csv, runs/K-C7-retention-bytes_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 18  K-C7-retention-bytes_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      K-C7-retention-bytes - A byte bound instead of a time bound: retention.
tests     F3: does the size of a bound, or the broker enforcing it, decide how predictable the moment of removal is
broker    Kafka 4.3.1
bound     retention 4 MiB per partition; retention 604800 s; segment size 1 MiB
outcome   660000 published: 469621 in time, 0 too late, 190379 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 9.08 -> 11.17 ms, 0 publishes lost
drain     31 s measured vs 39 s predicted by B/(mu-lambda)
accounting balanced
files     runs/K-C7-retention-bytes_f5_s360_A_r3.timeline.csv, runs/K-C7-retention-bytes_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 19  R-C1-unbounded_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 453989 in time, 32405 too late, 42171 lost while useful, 44611 lost after deadline, 0 refused, 86824 never sent
alarm     6 episodes, 38% of the slow phase, 2 purge(s)
neighbour confirm p99 17.82 -> 9873.61 ms, 2356 publishes lost
drain     47 s measured vs 42 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r1.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 20  R-C1-unbounded_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 453401 in time, 33894 too late, 38960 lost while useful, 48553 lost after deadline, 0 refused, 85192 never sent
alarm     7 episodes, 38% of the slow phase, 2 purge(s)
neighbour confirm p99 4.4 -> 9778.9 ms, 2002 publishes lost
drain     50 s measured vs 46 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r2.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 21  R-C1-unbounded_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 451677 in time, 30965 too late, 54867 lost while useful, 43229 lost after deadline, 0 refused, 79262 never sent
alarm     6 episodes, 37% of the slow phase, 2 purge(s)
neighbour confirm p99 4.4 -> 9778.33 ms, 2750 publishes lost
drain     49 s measured vs 47 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s360_A_r3.timeline.csv, runs/R-C1-unbounded_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 22  R-C1-unbounded_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 232599 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 7401 never sent
alarm     1 episodes, 11% of the slow phase, 0 purge(s)
neighbour confirm p99 4.99 -> 9197.0 ms, 1480 publishes lost
drain     49 s measured vs 44 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r1.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 23  R-C1-unbounded_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 231272 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 8728 never sent
alarm     1 episodes, 10% of the slow phase, 0 purge(s)
neighbour confirm p99 4.48 -> 9195.52 ms, 1494 publishes lost
drain     48 s measured vs 45 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r2.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 24  R-C1-unbounded_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C1-unbounded - No bound at all: the configuration people reach for when they add a queue to avoid losing work.
tests     H2: a queue bounded only by broker resources degrades the whole node
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   240000 published: 231270 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 0 refused, 8730 never sent
alarm     1 episodes, 11% of the slow phase, 0 purge(s)
neighbour confirm p99 4.19 -> 9202.46 ms, 1452 publishes lost
drain     48 s measured vs 44 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C1-unbounded_f5_s90_B_r3.timeline.csv, runs/R-C1-unbounded_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 25  R-C2-maxlen-drophead_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467246 in time, 0 too late, 192754 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 13.36 -> 20.47 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r1.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 26  R-C2-maxlen-drophead_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467457 in time, 0 too late, 192543 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 4.37 -> 5.23 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r2.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 27  R-C2-maxlen-drophead_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 467548 in time, 0 too late, 192452 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 4.61 -> 4.38 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s360_A_r3.timeline.csv, runs/R-C2-maxlen-drophead_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 28  R-C2-maxlen-drophead_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 209767 in time, 0 too late, 30233 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 17.06 -> 42.81 ms, 0 publishes lost
drain     36 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r1.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 29  R-C2-maxlen-drophead_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 209733 in time, 0 too late, 30267 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 17.65 -> 43.21 ms, 0 publishes lost
drain     33 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r2.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 30  R-C2-maxlen-drophead_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C2-maxlen-drophead - Length bound with the default overflow policy, which silently discards the oldest messages.
tests     H1/H2: bounding by length avoids the alarm but still loses useful work
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   240000 published: 209763 in time, 0 too late, 30237 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 18.13 -> 39.68 ms, 0 publishes lost
drain     33 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C2-maxlen-drophead_f5_s90_B_r3.timeline.csv, runs/R-C2-maxlen-drophead_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 31  R-C3-reject-publish_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363364 in time, 104200 too late, 0 lost while useful, 0 lost after deadline, 192436 refused, 0 never sent
alarm     none
neighbour confirm p99 3.45 -> 3.39 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r1.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 32  R-C3-reject-publish_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363398 in time, 104193 too late, 0 lost while useful, 0 lost after deadline, 192409 refused, 0 never sent
alarm     none
neighbour confirm p99 5.2 -> 4.58 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r2.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 33  R-C3-reject-publish_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 363499 in time, 104131 too late, 0 lost while useful, 0 lost after deadline, 192370 refused, 0 never sent
alarm     none
neighbour confirm p99 4.24 -> 4.0 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s360_A_r3.timeline.csv, runs/R-C3-reject-publish_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 34  R-C3-reject-publish_f5_s90_B_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 209928 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 30072 refused, 0 never sent
alarm     none
neighbour confirm p99 4.47 -> 4.52 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r1.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 35  R-C3-reject-publish_f5_s90_B_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 209929 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 30071 refused, 0 never sent
alarm     none
neighbour confirm p99 4.31 -> 4.46 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r2.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 36  R-C3-reject-publish_f5_s90_B_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C3-reject-publish - Length bound that refuses the publisher instead of discarding, so the producer learns about it and can decide.
tests     H2/H3: refusal keeps the broker and the neighbour healthy
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   240000 published: 209924 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 30076 refused, 0 never sent
alarm     none
neighbour confirm p99 4.28 -> 4.44 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C3-reject-publish_f5_s90_B_r3.timeline.csv, runs/R-C3-reject-publish_f5_s90_B_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 37  R-C4-ttl_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 483241 in time, 400 too late, 124898 lost while useful, 0 lost after deadline, 0 refused, 51461 never sent
alarm     7 episodes, 22% of the slow phase, 1 purge(s)
neighbour confirm p99 4.39 -> 9649.47 ms, 2402 publishes lost
drain     42 s measured vs 44 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r1.timeline.csv, runs/R-C4-ttl_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 38  R-C4-ttl_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 482522 in time, 1155 too late, 129284 lost while useful, 0 lost after deadline, 0 refused, 47039 never sent
alarm     7 episodes, 25% of the slow phase, 1 purge(s)
neighbour confirm p99 3.36 -> 9645.48 ms, 3509 publishes lost
drain     43 s measured vs 40 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r2.timeline.csv, runs/R-C4-ttl_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 39  R-C4-ttl_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C4-ttl - Age bound: messages expire once they are too old to be useful, whatever the queue length.
tests     H1: an age bound converts useful loss into expired loss
broker    RabbitMQ 4.3.6
bound     age bound: messages expire after 54 s; classic queue, durable, persistent messages
outcome   660000 published: 487898 in time, 3040 too late, 122827 lost while useful, 0 lost after deadline, 0 refused, 46235 never sent
alarm     9 episodes, 26% of the slow phase, 1 purge(s)
neighbour confirm p99 3.88 -> 9673.76 ms, 2121 publishes lost
drain     50 s measured vs 48 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C4-ttl_f5_s360_A_r3.timeline.csv, runs/R-C4-ttl_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 40  R-C5-combined_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467554 in time, 0 too late, 12501 lost while useful, 0 lost after deadline, 179945 refused, 0 never sent
alarm     none
neighbour confirm p99 3.7 -> 3.6 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r1.timeline.csv, runs/R-C5-combined_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 41  R-C5-combined_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467601 in time, 0 too late, 12500 lost while useful, 0 lost after deadline, 179899 refused, 0 never sent
alarm     none
neighbour confirm p99 4.0 -> 3.89 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r2.timeline.csv, runs/R-C5-combined_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 42  R-C5-combined_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C5-combined - Age bound, publisher refusal and a consumer-side deadline check together.
tests     H1/H2/H3: the combination, measured against each part
broker    RabbitMQ 4.3.6
bound     length bound 24000 messages, overflow policy reject-publish; age bound: messages expire after 54 s; classic queue, durable, persistent messages; consumer discards messages already past their deadline
outcome   660000 published: 467632 in time, 0 too late, 12500 lost while useful, 0 lost after deadline, 179868 refused, 0 never sent
alarm     none
neighbour confirm p99 4.55 -> 3.98 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C5-combined_f5_s360_A_r3.timeline.csv, runs/R-C5-combined_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 43  R-C9-wide-headroom_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 407229 in time, 122179 too late, 19807 lost while useful, 74994 lost after deadline, 0 refused, 35791 never sent
alarm     2 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 3.27 -> 9474.2 ms, 3147 publishes lost
drain     90 s measured vs 86 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r1.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 44  R-C9-wide-headroom_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 404656 in time, 124065 too late, 18405 lost while useful, 74459 lost after deadline, 0 refused, 38415 never sent
alarm     5 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 13.5 -> 9583.74 ms, 3611 publishes lost
drain     98 s measured vs 86 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r2.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 45  R-C9-wide-headroom_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C9-wide-headroom - Unbounded, with twice the disk headroom of R-C1: 120 MiB instead of 60.
tests     F6: more headroom postpones the alarm without preventing it
broker    RabbitMQ 4.3.6
bound     no bound of any kind; classic queue, durable, persistent messages
outcome   660000 published: 406640 in time, 120228 too late, 18159 lost while useful, 74624 lost after deadline, 0 refused, 40349 never sent
alarm     4 episodes, 17% of the slow phase, 1 purge(s)
neighbour confirm p99 14.55 -> 9593.52 ms, 2245 publishes lost
drain     99 s measured vs 84 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C9-wide-headroom_f5_s360_A_r3.timeline.csv, runs/R-C9-wide-headroom_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 46  R-C11-reject-correct_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 457912 in time, 9285 too late, 0 lost while useful, 0 lost after deadline, 192803 refused, 0 never sent
alarm     none
neighbour confirm p99 3.98 -> 3.46 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r1.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 47  R-C12-maxlen-correct-wide_f5_s360_A_r1  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399307 in time, 103977 too late, 156716 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 5.14 -> 5.02 ms, 0 publishes lost
drain     63 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r1.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r1.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 48  R-C11-reject-correct_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 467310 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 192690 refused, 0 never sent
alarm     none
neighbour confirm p99 5.23 -> 4.41 ms, 0 publishes lost
drain     26 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r2.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 49  R-C12-maxlen-correct-wide_f5_s360_A_r2  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399268 in time, 103997 too late, 156735 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 4.64 -> 4.63 ms, 0 publishes lost
drain     63 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r2.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r2.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 50  R-C11-reject-correct_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C11-reject-correct - 
broker    RabbitMQ 4.3.6
bound     length bound 23700 messages, overflow policy reject-publish; classic queue, durable, persistent messages
outcome   660000 published: 467312 in time, 0 too late, 0 lost while useful, 0 lost after deadline, 192688 refused, 0 never sent
alarm     none
neighbour confirm p99 4.63 -> 4.2 ms, 0 publishes lost
drain     25 s measured vs 24 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C11-reject-correct_f5_s360_A_r3.timeline.csv, runs/R-C11-reject-correct_f5_s360_A_r3.messages.csv.gz
--------------------------------------------------------------------------------------
RUN 51  R-C12-maxlen-correct-wide_f5_s360_A_r3  [OK]
--------------------------------------------------------------------------------------
case      R-C12-maxlen-correct-wide - 
broker    RabbitMQ 4.3.6
bound     length bound 59750 messages, overflow policy drop-head (the default); classic queue, durable, persistent messages
outcome   660000 published: 399370 in time, 103923 too late, 156707 lost while useful, 0 lost after deadline, 0 refused, 0 never sent
alarm     none
neighbour confirm p99 4.99 -> 5.12 ms, 0 publishes lost
drain     62 s measured vs 60 s predicted by B/(mu-lambda)
accounting balanced
files     runs/R-C12-maxlen-correct-wide_f5_s360_A_r3.timeline.csv, runs/R-C12-maxlen-correct-wide_f5_s360_A_r3.messages.csv.gz
