FROM neo4j:5.22.0

RUN apt-get update && apt-get -y dist-upgrade

RUN apt-get install -y bash curl

RUN mkdir /export
RUN chown 7474:7474 /export

RUN cp /var/lib/neo4j/labs/apoc-*.jar /var/lib/neo4j/plugins

# Neosemantics
#RUN curl -L https://github.com/neo4j-labs/neosemantics/releases/download/5.20.0/neosemantics-5.20.0.jar > /var/lib/neo4j/plugins/neosemantics-5.20.0.jar
#ENV NEO4J_server_unmanaged__extension__classes="n10s.endpoint=/rdf"

ENV NEO4J_apoc_export_file_enabled=TRUE
ENV NEO4J_dbms_security_auth__enabled=FALSE
ENV NEO4J_dbms_security_procedures_unrestricted="my_extensions_example,my_procedures_*"
ENV NEO4J_dbms_security_procedures_allowlist="apoc.coll.*,apoc.load.*,gds.*,apoc.*"
ENV NEO4J_dbms_security_procedures_unrestricted="apoc.*"
ENV NEO4J_dbms_databases_default__to__read__only=FALSE
ENV NEO4J_dbms_logs_http_enabled=TRUE
ENV NEO4J_server_logs_gc_enabled=TRUE
ENV NEO4J_server_logs_gc_options="-Xlog:gc*,safepoint,age*=trace"
ENV NEO4J_server_threads_worker__count=4
ENV NEO4J_server_bolt_enabled=TRUE
ENV NEO4J_server_http_enabled=TRUE
ENV NEO4J_server_memory_heap_initial__size="10g"
ENV NEO4J_server_memory_heap_max__size="10g"
ENV NEO4J_dbms_usage__report_enabled=FALSE
ENV NEO4J_dbms_security_auth__enabled=FALSE
ENV NEO4J_server_directories_data="/mnt/repo/neo4j-data"
ENV NEO4J_server_directories_logs="/mnt/repo/neo4j-logs"
ENV NEO4J_server_directories_transaction_logs_root="/mnt/repo/neo4j-data/transactions"
ENV NEO4J_apoc_export_file_enabled=TRUE
ENV NEO4J_apoc_import_file_use__neo4j__config=FALSE


# Copy scripts
RUN mkdir -p /scripts
WORKDIR /scripts
COPY ./export-data.sh ./export-data.sh


RUN chmod +x ./export-data.sh

ENTRYPOINT ["/scripts/export-data.sh"]


