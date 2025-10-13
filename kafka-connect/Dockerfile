FROM confluentinc/cp-kafka-connect:7.6.1

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.7.4
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.3.jar

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.5
