FROM qacompany/qendpoint

ARG QVERSION="1.16.1"


RUN apt-get update && apt-get install -y unzip python3

WORKDIR /app

RUN wget https://github.com/the-qa-company/qEndpoint/releases/download/v${QVERSION}/qendpoint-cli.zip

RUN unzip qendpoint-cli.zip


WORKDIR /app/qendpoint-cli-${QVERSION}/bin/

RUN chmod +x /app/qendpoint-cli-${QVERSION}/bin/

ENV PATH=/app/qendpoint-cli-${QVERSION}/bin/:${PATH}

ENV JENA_SHA512 f426275591aaa5274a89cab2f2ee16623086c5f0c7669bda5b2cead90089497e57098885745fd88e3c7db75cbaac48fe58f84ec9cd2dbb937592ff2f0ef0f92e
ENV JENA_VERSION 5.1.0
ENV ASF_MIRROR https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=
ENV ASF_ARCHIVE https://archive.apache.org/dist/

RUN echo "$JENA_SHA512  jena.tar.gz" > jena.tar.gz.sha512

# Download/check/unpack/move in one go (to reduce image size)
RUN     (curl --location --silent --show-error --fail --retry-connrefused --retry 3 --output jena.tar.gz ${ASF_MIRROR}jena/binaries/apache-jena-$JENA_VERSION.tar.gz || \
         curl --fail --silent --show-error --retry-connrefused --retry 3 --output jena.tar.gz $ASF_ARCHIVE/jena/binaries/apache-jena-$JENA_VERSION.tar.gz) && \
	sha512sum -c jena.tar.gz.sha512 && \
	tar zxf jena.tar.gz && \
	mv apache-jena* /jena && \
	rm jena.tar.gz* && \
	cd /jena && rm -rf *javadoc* *src* bat

# Add to PATH
ENV PATH $PATH:/app/qendpoint-cli-${QVERSION}/bin/:/jena/bin
# Check it works
RUN riot --version
RUN rdf2hdt.sh --help

RUN apt-get install python3-pip -y
COPY ./requirements.txt /requirements.txt
RUN python3 -m pip install -r /requirements.txt
RUN rm -f /requirements.txt

COPY process_graph.py /bin/process_graph.py

COPY ./convert-all.sh /bin/convert-all.sh

RUN chmod +x /bin/convert-all.sh

ENTRYPOINT ["/bin/convert-all.sh"]

