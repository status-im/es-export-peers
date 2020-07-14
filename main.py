#!/usr/bin/env python3
from os import path
from optparse import OptionParser

from query import ESQueryPeers
from postgres import PGDatabase

HELP_DESCRIPTION = 'This generates a CSV with buckets of peer_ids for every day.'
HELP_EXAMPLE = 'Example: ./unique_count.py -i "logstash-2019.11.*" -f peer_id'


def parse_opts():
    parser = OptionParser(description=HELP_DESCRIPTION, epilog=HELP_EXAMPLE)
    parser.add_option('-H', '--es-host', default='localhost',
                      help='ElasticSearch host.')
    parser.add_option('-P', '--es-port', default=9200,
                      help='ElasticSearch port.')
    parser.add_option('-d', '--db-host', default='localhost',
                      help='PostgreSQL host.')
    parser.add_option('-b', '--db-port', default=5432,
                      help='PostgreSQL port.')
    parser.add_option('-u', '--db-user', default='postgres',
                      help='PostgreSQL user.')
    parser.add_option('-p', '--db-pass', default='postgres',
                      help='PostgreSQL password.')
    parser.add_option('-n', '--db-name', default='postgres',
                      help='PostgreSQL database name.')
    parser.add_option('-i', '--index-pattern', default='logstash-*',
                      help='Patter for matching indices.')
    parser.add_option('-f', '--field', type='str', default='peer_id',
                      help='Name of the field to count.')
    parser.add_option('-m', '--max-size', type='int', default=100000,
                      help='Max number of counts to find.')
    (opts, args) = parser.parse_args()

    if not opts.field:
        parser.error('No field name specified!')

    return (opts, args)


def main():
    (opts, args) = parse_opts()

    esq = ESQueryPeers(
        opts.es_host,
        opts.es_port
    )
    psg = PGDatabase(
        opts.db_name,
        opts.db_user,
        opts.db_pass,
        opts.db_host,
        opts.db_port
    )

    days = psg.get_present_days()
    present_indices = ['logstash-{}'.format(d.replace('-', '.')) for d in days]

    peers = []
    for index in esq.get_indices(opts.index_pattern):
        if index in present_indices:
            continue
        print('Index: {}'.format(index))
        peers.extend(esq.get_peers(index, opts.field, opts.max_size))

    if len(peers) == 0:
        print('Nothing to insert into database.')
        exit(0)

    rval = psg.inject_peers(peers)
    print(rval)

if __name__ == '__main__':
    main()
