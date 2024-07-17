---
title: Extend Mirabelle
weight: 8
disableToc: false
---

It's possible in Mirabelle to define custom actions and custom I/O. This allows you to not be limited by the Mirabelle DSL avec integrations by adding your own code.

## Clojure

Mirabelle is built with [Clojure](https://clojure.org/), Extending it should also be done in Clojure.

Here are some resources to learn the basics of Clojure:

- Aphyr did a [serie of articles](https://aphyr.com/tags/Clojure-from-the-ground-up) about Clojure.
- The Riemann documentation has [a tutorial](http://riemann.io/clojure.html) on Clojure.
- [Brave clojure](https://www.braveclojure.com/clojure-for-the-brave-and-true/) is a book available for free online.
- I wrote a Clojure interactive tutorial (in French), which is available [Here](https://www.tour.mcorbin.fr/).

## Dependencies

You will need to install:

- [Leiningen](https://leiningen.org/), the Clojure build tool.
- Java. Mirabelle is tested under Java 11 (LTS).

## Initialize the project

Once Leiningen is installed, you can initialize a new Clojure project (replace the latest parameter by your module name):

```
lein new app mymodule
```

Now, open the `project.clj` file in the `my-module` directory.

First, remove the `:main ^:skip-aot my-module-name.core` line. Then extend the `:dependencies` list of the Mirabelle project file by adding `[fr.mcorbin/mirabelle "<replace-with-your-mirabelle-version>"]`. Be sure to use the same version as the Mirabelle you're using (it may in theory work with different versions but we never know).

Be sure to also remove the `org.clojure/clojure` dependency from the `:dependencies` list, Clojure being already provided by Mirabelle itself.

You can now open the file in `src/mymodule/core.clj`

## Write a custom action

In this example, we will create a simple action which will keep all events greater than a threshold, and remove the other events.

Here would be the code of the `core.clj` file:

```clojure
(ns mymodule.core
  (:require [mirabelle.action :as a]
            [mirabelle.io :as io]))

(defn keep-if-greater-than*
  [context threshold & children]
  (fn [event]
    (when (> (:metric event) threshold)
      (a/call-rescue event children))))

```

First, we `:require` the `mirabelle.action` and the `mirabelle.io` namespaces (the `io` one will be used later in this tutorial).

Then, we create a Clojure function named `keep-if-greater-than*`. This function takes 3 parameters:

- A `context` map, which is a Mirabelle internal component which contains some internal state. It will not be used here.
- `threshold`, which is the threshold parameter passed to the stream.
- `children`, which is a list of the next actions to be called.

Then, we check if the event `:metric` field is greater than the threshold. If yes, we call `(a/call-rescue event children)`.

`call-rescue` is a function taking an event (or a list of events), the list of children, and will forward the event to each child stream. In our case, we only forward them if the `:metric` field is greater than the threshold.

## Write a custom output

Outputs are stateful components which can be then referenced in Mirabelle to interact with external systems (timeseries databases, cloud services...).

Let's define a simple I/O which will write events into a path (you can add the code at the end of the `core.clj` file):

```clojure
(defrecord CustomFileOutput [registry path]
  io/Output
  (inject! [this events]
    (doseq [event events]
      (spit path (str (pr-str event) "\n") :append true))))
```

Here we create a clojure `record` which implements one `protocol`: `IO`.

This protocol has only one function, which receives a list of events. These events are then written to the file with the `path` location.

`registry` and `path` are fields passed to the record. `registry` is automatically injected by Mirabelle and is a [https://micrometer.io/](micrometer) registry, and can be used to add metrics on your I/O component.

The `path` parameter will be set by the user (it's explained a bit later in the documentation).

The I/O records can also implement the `Lifecycle` protocol from the [https://github.com/stuartsierra/component](component library). It's very useful in order to initialize some states for your component, and properly shut it down if needed.

## Use the custom action and the custom output in Mirabelle

Now that our code is ready, let's use it in Mirabelle.

You should first run `lein uberjar` at the root of your project in order to build the jar files.

Two files should be created in `target/uberjar/`:

- A standalone jar, named `mymodule-<version>-standalone.jar`
- A regular jar, named `mymodule-<version>.jar`

You will need to reference the standalone jar in Mirabelle if you added extra dependencies to your project. Otherwise you can use the regular one.

The first thing to do to use your module is to reference your new action and I/O in the Mirabelle [configuration files](/howto/configuration/).

```clojure
{:stream {:directories ["/etc/mirabelle/streams"]
          :actions {:keep-if-greater-than mymodule.core/keep-if-greater-than*}}
 :outputs {:custom-file {:type :custom
                         :builder mymodule.core/map->CustomFileOutput
                         :config {:path "/tmp/bar"}}}}
```

You can see that the `:keep-if-greater-than*` key references the function you wrote in your module, and that the `:custom-file` Output references `my-module-name.core/map->CustomFileOutput` in `:builder` (the `map->CustomFileOutput` function is automatically available, it's how Clojure records work).

You can also write a stream which uses both your new action and the new output you declared:

```clojure
(streams
 (stream
  {:name :foo :default true}
  (info)
  (custom :keep-if-greater-than [5]
    (output! :custom-file))))
```

You should now launch Mirabelle and include your module jar in the command:

```
java -cp "mirabelle.jar:your-module.jar" mirabelle.core
```

Mirabelle should be running, and if you push an event with a metric greater than 5 it should be written into the `/tmp/custom` path.

