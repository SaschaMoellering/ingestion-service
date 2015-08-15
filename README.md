# ingestion-service
A simple Vert.x 3 based service to ingest data into Kafka/Kinesis

To compile protobuf: protoc -I=./src/main/proto --java_out=src/main/java ./src/main/proto/addressbook.proto