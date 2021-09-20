---
title: Install, build and launch Mirabelle
weight: 1
disableToc: false
---

# Build Mirabelle

In order to build Mirabelle, you need to install:

- [Leiningen](https://leiningen.org/), the Clojure build tool.
- Java. Mirabelle is tested under Java 11 (LTS).

Then, clone the Mirabelle [Git repository](https://github.com/mcorbin/mirabelle). You can now build the project with `lein uberjar`.

The resulting jar will be in `target/uberjar/mirabelle-<version>-standalone.jar`

# Launch mirabelle

## Using Java

Mirabelle needs a [configuration file](/howto/configuration/). The path to this configuration file should be referenced by the `MIRABELLE_CONFIGURATION` environment variable.

Releases are available on [Github](https://github.com/mcorbin/mirabelle/releases).

You can then run Mirabelle by launching `java -jar mirabelle.jar`.

## Using Docker

Let's say your I/O are in `/etc/mirabelle/io`, your EDN (compiled) streams in `/etc/mirabelle/streams`, your configuration file in `/etc/mirabelle/config.edn`. The configuration file contains:

```clojure
{:tcp {:host "0.0.0.0"
       :port 5555
       :native? true}
 :http {:host "0.0.0.0"
        :port 5558}
 :websocket {:host "0.0.0.0"
             :port 5556}
 :stream {:directories ["/streams"]
          :actions {}}
 :io {:directories ["/io"]
      :custom {}}
 :logging {:level "info"
           :console {:encoder :json}}}
```

You should now be able to launch Mirabelle using Docker (you can check the [Docker hub](https://hub.docker.com/r/mcorbin/mirabelle/tags) to get the latest release):

```
docker run -p 5555:5555 -p 5556:5556 -p 5558:5558 \
-v /etc/mirabelle/io:/io \
-v /etc/mirabelle/streams:/streams \
-v /etc/mirabelle/config.edn:/config/config.edn \
-e MIRABELLE_CONFIGURATION=/config/config.edn \
mcorbin/mirabelle:v0.5.0
```

## Using Leiningen

`lein run` should work. The configuration used is the file `dev/resources/config.edn`. You should replace the path in this file by your own paths.
