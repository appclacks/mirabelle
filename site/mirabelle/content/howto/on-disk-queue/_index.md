---
title: On Disk queue
weight: 30
chapter: true
---

*Warning*: I'm not totally satisfied with this feature and with the current implementation. It will change in the future. Please provide me feedbacks ;)

Mirabelle allows you in its [configuration file](/howto/configuration/) to optionally persist some events into a queue written on disk. The queue implementation is a [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue).

You can configure the queue roll cycle in the Mirabelle configuration. The default is `:half-hourly`. More information about roll cycles can be found [here](https://github.com/OpenHFT/Chronicle-Queue#the-maximum-number-of-messages-per-cycle).

Note: Mirabelle will **not** rotate/delete old files for you. You need to do it yourself.

```clojure
(where [:= :service "foo"]
  (disk-queue!))
```

In this example, Mirabelle will write all events with `:service` "foo" on disk.

Why is it useful ? You can use the queue for:

- Archive events. You could have a cronjob taking all events of the day and putting them into S3 for example. You could then reuse these events for example to [reinject them later](/production/#use-cases) into Mirabelle.
- Rebuilding states. Mirabelle streams will not [keep their states](/production/#fault-tolerance) if Mirabelle crashes. But when Mirabelle starts, all events from the ondisk queue are reinjected in the order they are in the queue into Mirabelle.

The reinjected events will be tagged "discard". All stateful actions (I/O, pubsub, logger) will *not* be executed for discarded events. It means the events will rebuild all streams internal states but without producing side effects.

It's nice, but it's not perfect. The two big limitations today are:

- Events will only be reinjected into the `:default` streams. So be careful about that, the queue will not work with other streams. I will work on that.
- It can be slow. What if you have 10 millions events in the queue ? Even if you process them at 100 000 events per seconds, it will take 100 seconds before Mirabelle would be fully started. Do you really want to wait minutes for your service to start ?

This feature will probably change in the future, i'm thinking hard about how to rebuild streams states quickly and without sacrificing streams performances. Ideas are welcomed.
