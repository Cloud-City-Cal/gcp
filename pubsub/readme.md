# PubSub

This folder contains the schema definitions used for Pub/Sub (which is further passed into BigQuery). You'll need to have a [ProtoBuf](https://github.com/protocolbuffers/protobuf) compiler in order to run the following commands.


## BigQuery

To regenerate the schema for BigQuery, install [protoc-gen-bq-schema](https://github.com/GoogleCloudPlatform/protoc-gen-bq-schema), uncomment the `import` and `option` lines in the files, and run
```bash
protoc --proto_path=/path/to/protoc-gen-bq-schema --proto_path=. --bq-schema_out=. xxxx.proto
```

## Cloud Function

To regenerate the `*_pb2.py` files for Cloud Functions, run
```bash
protoc --proto_path=. --python_out=. xxxx.proto
```