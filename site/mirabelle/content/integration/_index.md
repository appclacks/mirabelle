---
title: Clients and integrations
weight: 13
chapter: false
---

All [Riemann tooling](http://riemann.io/clients.html) should in theory work with Mirabelle.

I will test with Mirabelle the Riemann integrations one by one and update this page later.

## Emacs

You can put that in your emacs configuration in order to have a good indentation for Mirabelle actions (as you can see, only a few actions are list, I will put the other ones later):

```
(put-clojure-indent 'above-dt 2)
(put-clojure-indent 'by 1)
(put-clojure-indent 'coalesce 2)
(put-clojure-indent 'coll-percentiles 1)
(put-clojure-indent 'changed 2)
(put-clojure-indent 'fixed-time-window 1)
(put-clojure-indent 'not-expired 0)
(put-clojure-indent 'over 1)
(put-clojure-indent 'rate 0)
(put-clojure-indent 'sflatten 0)
(put-clojure-indent 'sformat 3)
(put-clojure-indent 'stream 1)
(put-clojure-indent 'with 1)
(put-clojure-indent 'where 1)
```
