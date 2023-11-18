# PubSub

This folder contains the schema definitions used for Pub/Sub (which is further passed into BigQuery). 


## BigQuery

To regenerate the schema for BigQuery, install [protoc-gen-bq-schema](https://github.com/GoogleCloudPlatform/protoc-gen-bq-schema), and run
```bash
protoc --proto_path=~/Documents/Github/protoc-gen-bq-schema --proto_path=. --bq-schema_out=. bay_area_511_event.proto
```
