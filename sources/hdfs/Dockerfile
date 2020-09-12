FROM openjdk:8

ARG version=hadoop-3.3.0

RUN apt-get update \
    && apt-get -y upgrade

RUN apt-get install -y ssh \
                       rsync \
                       curl

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 0600 ~/.ssh/authorized_keys

RUN curl https://downloads.apache.org/hadoop/common/$version/$version.tar.gz \
    | tar -xzvC /usr/local/ 

WORKDIR /usr/local/$version

COPY sources/core-site.xml etc/hadoop/
COPY sources/hdfs-site.xml etc/hadoop/
COPY sources/hadoop-env.sh etc/hadoop/

RUN bin/hdfs namenode -format

ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root

EXPOSE 9000 \
       9864 \
       9870 

CMD service ssh restart \
    && sbin/start-dfs.sh \
    && sleep infinity
