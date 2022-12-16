FROM registry.access.redhat.com/ubi7/ubi

# Build Dependencies
RUN yum install -y wget
# Install Java
RUN mkdir -p /opt/java \
    && cd /opt/java \
    && wget https://download.oracle.com/java/17/archive/jdk-17.0.5_linux-x64_bin.tar.gz \
    && tar xvzf jdk-17.0.5_linux-x64_bin.tar.gz \
    && ln -s /opt/java/jdk-17.0.5/bin/java /usr/bin/java

# Clean up
RUN rm -f jdk-17.0.5_linux-x64_bin.tar.gz

# Install Maven
RUN mkdir -p /opt/maven \
    && cd /opt/maven \
    && wget https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz \
    && tar xvzf apache-maven-3.8.6-bin.tar.gz \
    && ln -s /opt/maven/apache-maven-3.8.6/bin/mvn /usr/bin/mvn

# Clean up
RUN rm -f apache-maven-3.8.6-bin.tar.gz

# Install java-iceberg-cli
COPY tools/java-iceberg-cli /home/java-iceberg-cli
RUN cd /home/java-iceberg-cli \
    && mvn package

# Clean up
RUN rm -rf /tmp/*

ENTRYPOINT ["tail", "-f", "/dev/null"]