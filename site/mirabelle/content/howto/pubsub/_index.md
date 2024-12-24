---
title: Publish Subscribe
weight: 14
disableToc: false
---

The Mirabelle Websocket server allows user to subscribe to channels.

## An example

```clojure
(streams
 (stream {:name :bar :default true}
  (where [:= :service "bar"]
    (publish! :my-channel))))
```

In this stream, we filter all events with `:service` "bar", then we publish them to a channel named `:my-channel`.

Users then subscripe to those channels. In this example, all users subscribing to the `:my-channel` channel will receive the events.

You can test that yourself by running the `websocket.py` script available [here](https://github.com/mcorbin/mirabelle/tree/master/pubsub). You will need Python 3, and to install the dependencies listed in `requirements.txt` using `pip install -r requirements.txt`.

When you subscribe to a channel, you should provide a valid [where clause](/howto/stream/#filtering-events) in base64. For example, the query `[:always-true]` which matches everything would be `WzphbHdheXMtdHJ1ZV0=`.

You can now run the script with `./websocket.py`. You can specify a channel with `--channel` (like `--channel my-channel` here), or a query with `--query` (the query will be automatically converted to base64 by the script).

You can also write your own scripts in various languages. You need to subscribe to `ws://<host>:<port>/channel/<channel-name>?query=<query>`.
