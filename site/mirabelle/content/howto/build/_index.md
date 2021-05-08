---
title: Install or build Mirabelle
weight: 1
disableToc: false
---

# Build Mirabelle

In order to build Mirabelle, you need to install:

- [Leiningen](https://leiningen.org/), the Clojure build tool.
- Java. Mirabelle is tested under Java 11 (LTS).

Then, close the Mirabelle [Git repository](https://github.com/mcorbin/mirabelle). You can now build the project with `lein uberjar`.

The resulting jar will be in `target/uberjar/mirabelle-<version>-standalone.jar`

# Launch mirabelle

## Using Java

Mirabelle needs a [configuration file](/howto/configuration/). The path to this configuration file should be referenced by the `MIRABELLE_CONFIGURATION` environment variable.

Releases are available on [Github](https://github.com/mcorbin/mirabelle/releases).

You can then run Mirabelle by launching `java -jar mirabelle.jar`.

## Using Docker

TODO


## Using Leiningen

`lein run` should work. The configuration used is the file `dev/resources/config.yaml`.

