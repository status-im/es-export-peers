# Description

This Python script queries for unique Status peers and pushes them to an SQL database.

# Details

The script queries an ElasticSearch endpoint for `logstash-*` indices and aggregates counts of instances of log messages with set `peer_id` field.

This data is pushed to a PostgreSQL database in the following format:
```
peers=> \d peers;
                       Table "public.peers"
┌────────┬───────────────────────┬───────────┬──────────┬─────────┐
│ Column │         Type          │ Collation │ Nullable │ Default │
├────────┼───────────────────────┼───────────┼──────────┼─────────┤
│ date   │ date                  │           │          │         │
│ peer   │ character varying(64) │           │          │         │
│ count  │ integer               │           │          │         │
└────────┴───────────────────────┴───────────┴──────────┴─────────┘
```

# Usage

The `main.py` exposes several flags:
```
Usage: main.py [options]

This generates a CSV with buckets of peer_ids for every day.

Options:
  -h, --help            show this help message and exit
  -H ES_HOST, --es-host=ES_HOST
                        ElasticSearch host.
  -P ES_PORT, --es-port=ES_PORT
                        ElasticSearch port.
  -d DB_HOST, --db-host=DB_HOST
                        PostgreSQL host.
  -b DB_PORT, --db-port=DB_PORT
                        PostgreSQL port.
  -u DB_USER, --db-user=DB_USER
                        PostgreSQL user.
  -p DB_PASS, --db-pass=DB_PASS
                        PostgreSQL password.
  -n DB_NAME, --db-name=DB_NAME
                        PostgreSQL database name.
  -i INDEX_PATTERN, --index-pattern=INDEX_PATTERN
                        Patter for matching indices.
  -f FIELD, --field=FIELD
                        Name of the field to count.
  -F FLEET, --fleet=FLEET
                        Name of the fleet to query.
  -D PROGRAM, --program=PROGRAM
                        Name of the program to query.
  -m MAX_SIZE, --max-size=MAX_SIZE
                        Max number of counts to find.
  -l LOG_LEVEL, --log-level=LOG_LEVEL
                        Level of logging.

Example: ./unique_count.py -i "logstash-2019.11.*" -f "peer_id"
```

# Example

```
peers=> select * from peers limit 3;
┌────────────┬──────────────────────────────────────────────────────────────────┬───────┐
│    date    │                               peer                               │ count │
├────────────┼──────────────────────────────────────────────────────────────────┼───────┤
│ 2020-06-01 │ a18d4417b1d2fbddd7f9474250f703ba20472be5e1131bc09e35e9b18c1a5bf7 │  1300 │
│ 2020-06-01 │ 7dba96249159cef53fbb5ec010c2d7799fec7dcaf8b1d9754559ce9fbd463328 │   652 │
│ 2020-06-01 │ 3a13adfa4799f9505c83fab18d49a47f6de09344db3d96e18678c5d3c92f717e │   632 │
└────────────┴──────────────────────────────────────────────────────────────────┴───────┘
(3 rows)
```
