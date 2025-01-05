---
title: Publish Subscribe
weight: 14
disableToc: false
---

The Mirabelle Websocket server allows user to subscribe to channels.

This guide uses the [https://github.com/appclacks/cl](Appclacks CLI) to interact with Mirabelle.

## An example

```clojure
(streams
 (stream {:name :bar :default true}
  (where [:= :service "bar"]
    (publish! :my-channel))))
```

In this stream, we filter all events with `:service` "bar", then we publish them to a channel named `:my-channel`.

Users then subscripe to those channels. In this example, all users subscribing to the `:my-channel` channel will receive the events.

You can test that yourself by running the `appclacks mirabelle subscribe --channel my-channel` command. You can then send an event to Mirabelle (`mirabelle event send --name http_requests_duration_seconds --metric 1 --service bar`) and it should be displayed by the `subscribe` command.

When you subscribe to a channel, you should provide a valid [where clause](/howto/stream/#filtering-events) in base64. For example, the query `[:> :metric 10]` which matches everything would be `Wzo+IDptZXRyaWMgMTBd`. The CLI will do the conversion for you: `appclacks mirabelle subscribe --channel my-channel --query '[:> :metric 10]'` will keep only events with the `metric` field greater than 10.

You can also write your own scripts in various languages. You need to subscribe to `ws://<host>:<port>/channel/<channel-name>?query=<query>`.
