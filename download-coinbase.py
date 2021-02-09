import argparse
from datetime import datetime
import hashlib
import hmac
import json
import os
import pprint
import re
import sys
import time
import urllib.request


def auth_get(credentials, path, body=None, method=None):
    if not method:
        method = 'POST' if body else 'GET'

    COINBASE_SERVER = 'https://api.coinbase.com'
    url = COINBASE_SERVER + path
    timestamp = str(int(time.time()))
    message = timestamp + method + path + (body or '')
    signature = hmac.new(
        credentials['coinbase_secret'].encode('ascii'),
        message.encode('ascii'),
        hashlib.sha256).hexdigest()

    headers = {
        'Content-Type': 'application/json',
        'CB-ACCESS-KEY': credentials['coinbase_key'],
        'CB-ACCESS-SIGN': signature,
        'CB-ACCESS-TIMESTAMP': timestamp,
        'CB-VERSION': '2021-01-01',
    }

    request = urllib.request.Request(
        url=url, data=body, headers=headers, method=method)
    with urllib.request.urlopen(request) as handle:
        return json.load(handle)


def paginate_data(credentials, path, data='data', limit=float('inf')):
    next_uri = path
    count = 0
    while next_uri:
        result = auth_get(credentials, next_uri)
        for datum in result[data]:
            yield datum
            count += 1
            if count >= limit:
                return
        next_uri = result['pagination']['next_uri']


def download_transactions(credentials, dest_dir, break_at_seen=False):
    os.makedirs(dest_dir, exist_ok=True)
    for account in paginate_data(credentials, '/v2/accounts'):
        if account['currency']['code'] != 'BTC':
            continue
        print(account['id'], account['name'])
        account_path = os.path.join(
            dest_dir,
            'Coinbase-%s-%s.json' %
            (re.sub('[^a-zA-Z0-9-]', '_', account['name']), account['id']))
        if os.path.exists(account_path) and break_at_seen:
            with open(account_path) as fin:
                lines = [line for line in fin if not line.startswith('#')]
                data = json.loads(''.join(lines))
        else:
            data = {'account': account, 'transactions': {}}
        for transaction in paginate_data(
                credentials,
                '/v2/accounts/%s/transactions?limit=100' % account['id']):
            key = '%s-%s' % (transaction['created_at'], transaction['id'])
            if key not in data['transactions']:
                data['transactions'][key] = transaction
            else:
                break  # assume they're in order
        if data['transactions']:
            with open(account_path + '.tmp', 'w') as fout:
                fout.write(
                    '# Coinbase downloaded transactions (%s)\n' %
                    time.strftime('%Y-%m-%d %H:%M:%S'))
                json.dump(data, fout, sort_keys=True, indent=4)
            os.rename(account_path + '.tmp', account_path)


credentials_help = '''
No coinbase credentials found.

Please create a read-only API Key with

    wallet:transactions:read and wallet:accounts:read

permissions at https://www.coinbase.com/settings/api and either set the
environment variables COINBASE_API_KEY and COINBASE_SECRET or create a file
%s (--coinbase_credentials) with the following contents:

{
    "coinbase_key": "[COINBASE_API_KEY]",
    "coinbase_secret": "[COINBASE_SECRET]"
}
'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compute capital gains/losses.')
    parser.add_argument(
        '--coinbase_credentials_file', default='coinbase-credentials.json')
    parser.add_argument(
        'destination_dir', default='coinbase-downlaoded-transactions')
    parsed_args = parser.parse_args()

    if os.path.exists(parsed_args.coinbase_credentials_file):
        with open(parsed_args.coinbase_credentials_file) as fin:
            credentials = json.load(fin)
        assert 'coinbase_key' in credentials
        assert 'coinbase_secret' in credentials
    elif 'COINBASE_API_KEY' in os.environ and 'COINBASE_SECRET' in os.environ:
        credentials = {
            'coinbase_key': os.environ['COINBASE_API_KEY'],
            'coinbase_secret': os.environ['COINBASE_SECRET'],
        }
    else:
        print(credentials_help % parsed_args.coinbase_credentials_file)
        sys.exit(1)

    download_transactions(credentials, parsed_args.destination_dir)
