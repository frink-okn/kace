FROM neo4j:5.22.0

RUN apt-get update && apt-get -y dist-upgrade

RUN apt-get install -y bash curl

RUN mkdir /export
RUN chown 7474:7474 /export

RUN cp /var/lib/neo4j/labs/apoc-*.jar /var/lib/neo4j/plugins

RUN curl -qO /var/lib/neo4j/plugins/neosemantics-5.20.0.jar https://github.com/neo4j-labs/neosemantics/releases/download/5.20.0/neosemantics-5.20.0.jar

ENV NEO4J_apoc_export_file_enabled=TRUE
ENV NEO4J_dbms_security_auth__enabled=TRUE
ENV NEO4J_server_unmanaged__extension__classes="n10s.endpoint=/rdf"

USER 7474

