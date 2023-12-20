FROM clojure:temurin-21-lein-bullseye as build-env

ADD . /app
WORKDIR /app

RUN lein uberjar

# -----------------------------------------------------------------------------

from eclipse-temurin:21-jammy

RUN groupadd -g 10000 -r mirabelle && useradd -u 10000 -r -s /bin/false -g mirabelle mirabelle
RUN mkdir /app
COPY --from=build-env /app/target/uberjar/mirabelle-*-standalone.jar /app/mirabelle.jar

RUN chown -R 10000:10000 /app

user 10000

ENTRYPOINT ["java"]

CMD ["-jar", "/app/mirabelle.jar"]
