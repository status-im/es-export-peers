# Description

This Python script queries for unique Status peers and pushes them to an SQL database.

# Details

The script queries an ElasticSearch endpoint for `logstash-*` indices and aggregates counts of instances of log messages with set `peer_id` field.

This data is pushed to a PostgreSQL database in the following format:
```
```

# Example

```
```
