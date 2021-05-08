# Mirabelle

A stream processing engine, inspired by Riemann.

Work in progress, check https://mcorbin.fr/posts/2021-03-01-mirabelle-stream-processing/ for more information about the design.

Parts of this codebase are taken from the Riemann (https://riemann.io/) codebase, which is licensed under Eclipse Public License 1.0.


docker run -p 5555:5555 -p 5556:5556 -p 5558:5558 \
-v /etc/mirabelle/io:/io \
-v /etc/mirabelle/streams:/streams \
-v /etc/mirabelle/config.edn:/config/config.edn \
-e MIRABELLE_CONFIGURATION=/config/config.edn \
460b3e492eaa
