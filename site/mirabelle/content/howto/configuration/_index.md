---
title: Configuration
weight: 3
disableToc: false
---

# Mirabelle configuration

The Mirabelle configuration is defined in [EDN](https://github.com/edn-format/edn). Here is an example configuration file:

```clojure
{;; The Mirabelle TCP server. It's the server responsible for receiving events
 ;; using the Riemann format (protobuf).
 ;; the :key, :cert, and :cacert optional parameters can be set to enable TLS (the
 ;; parameters are path to the cert files)
 ;; The :event-executor-size optional parameter can be used to control the number
 ;; of threads for the TCP server (default to number of cores).
 ;; the :native? option can be used to enable epoll (on Linux) or kqueue (on mac or
 ;; freebsd), which should increase performances.
 :tcp {:host "0.0.0.0"
       :port 5555
       :native? true}

 ;; The Mirabelle HTTP Server.
 ;; The :key, :cert, and :cacert optional parameters can be set to enable TLS (the
 ;; parameters are path to the cert files)
 :http {:host "0.0.0.0"
        :port 5558}

 ;; The Mirabelle Websocket server
 :websocket {:host "0.0.0.0"
             :port 5556}

 ;; Optional parameter indicating the directory of which Mirabelle should
 ;; save queued events. Check this section of the documentation for more
 ;; information:
 :queue {:directory "/usr/lib/mirabelle/queue"}

 ;; The "real time" streams configuration.
 ;; The directories parameter is a list of directories containing streams definitions
 ;; The actions parameter can be used to define custom actions which can then
 ;; be used inside streams. Check the "On Disk queue" section of the
 ;; documentation for more information.
 :stream {:directories ["/usr/lib/mirabelle/streams/"]
          :actions {}}

 ;; I/O configurations.
 ;; The directories parameter is a list of directories containing I/O definitions
 ;; The custom parameter can be used to define custom actions.
 :io {:directories ["dev/resources/examples/io"]
      :custom {}}

 ;; Logging configuration (https://github.com/pyr/unilog)
 :logging {:level "info"
           :console {:encoder :json}}

  ;; Directories containing test files
 :test {:directories ["dev/resources/examples/streams/tests/"]}}
```

