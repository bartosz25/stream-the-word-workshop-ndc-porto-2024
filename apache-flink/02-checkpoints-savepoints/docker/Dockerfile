FROM flink:1.17.0-java11

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink
COPY apache-flink*.tar.gz /
RUN pip install grpcio-tools==1.46.0
RUN pip install apache-flink==1.17.0
RUN pip install apache-flink-libraries==1.17.0
