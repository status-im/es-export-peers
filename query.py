import hashlib
from elasticsearch import Elasticsearch


def remove_prefix(text, prefix):
    return text[text.startswith(prefix) and len(prefix):]


def hash_string(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


class ESQueryPeers():
    def __init__(self, host='localhost', port=9200, timeout=1200):
        self.client = Elasticsearch([{
            'host': host,
            'port': port,
        }],
                                    timeout=timeout,
                                    retry_on_timeout=True)
        self.cluster = self.client.info().get('cluster_name')

    def get_indices(self, pattern='logstash-*'):
        return self.client.indices.get(index=pattern).keys()

    def get_peers(self, index, field='peer_id', max_query=10000):
        body = {
            'size': 0,  # Don't return actual values
            'aggs': { 'peers': {
                'terms': {
                    'field': field,
                    'size': 10000,
                },
            }, },
        }
        # Query
        resp = self.client.search(index=index, body=body)
        aggs = resp.get('aggregations')

        # Collect results as list of dicts
        rval = []
        for bucket in aggs['peers']['buckets']:
            rval.append({
                'Date': remove_prefix(index, 'logstash-'),
                'Peer': hash_string(bucket['key']),
                'Count': bucket['doc_count'],
            })

        return rval
