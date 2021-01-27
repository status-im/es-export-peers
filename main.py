#!/usr/bin/env python3
from os import path
from datetime import datetime
from optparse import OptionParser

from log import setup_custom_logger
from query import ESQueryPeers
from postgres import PGDatabase

HELP_DESCRIPTION = 'This generates a CSV with buckets of peer_ids for every day.'
HELP_EXAMPLE = 'Example: ./unique_count.py -i "logstash-2019.11.*" -f "peer_id"'


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
    parser.add_option('-f', '--field', default='peer_id',
                      help='Name of the field to count.')
    parser.add_option('-F', '--fleet', default='eth.prod',
                      help='Name of the fleet to query.')
    parser.add_option('-D', '--program', default='docker/statusd-whisper-node',
                      help='Name of the program to query.')
    parser.add_option('-m', '--max-size', type='int', default=100000,
                      help='Max number of counts to find.')
    parser.add_option('-l', '--log-level', default='INFO',
                      help='Level of logging.')
    (opts, args) = parser.parse_args()

    if not opts.field:
        parser.error('No field name specified!')

    return (opts, args)


def main():
    (opts, args) = parse_opts()

    LOG = setup_custom_logger('root', opts.log_level.upper())

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

    LOG.info('Querying ES cluster for peers...')
    peers = []
    for index in esq.get_indices(opts.index_pattern):
        # skip already injected indices
        if index in present_indices:
            LOG.debug('Skipping existing index: %s', index)
            continue
        # skip current day as it's incomplete
        if index == datetime.now().strftime('logstash-%Y.%m.%d'):
            LOG.debug('Skipping incomplete current day.')
            continue
        LOG.info('Index: {}'.format(index))
        rval = esq.get_peers(
            index=index,
            field=opts.field,
            fleet=opts.fleet,
            program=opts.program,
            max_query=opts.max_size
        )
        if len(rval) == 0:
            LOG.warning('No entries found!')
        LOG.debug('Found: %s', len(rval))
        peers.extend(rval)

    if len(peers) == 0:
        LOG.info('Nothing to insert into database.')
        exit(0)

    LOG.info('Injecting peers data into database...')
    psg.inject_peers(peers)

if __name__ == '__main__':
    main()
