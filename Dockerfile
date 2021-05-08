FROM clojure:openjdk-11-lein as build-env

ADD . /app
WORKDIR /app

RUN lein uberjar

# -----------------------------------------------------------------------------

from openjdk:11

RUN groupadd -r mirabelle && useradd -r -s /bin/false -g mirabelle mirabelle
RUN mkdir /app
COPY --from=build-env /app/target/uberjar/mirabelle-*-standalone.jar /app/mirabelle.jar

RUN chown -R mirabelle:mirabelle /app

user mirabelle

ENTRYPOINT ["java"]

CMD ["-jar", "/app/mirabelle.jar"]
