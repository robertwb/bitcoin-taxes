#!/usr/bin/env python

"""
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Authors: Robert Bradshaw <robertwb@gmail.com>
"""

import abc
import argparse
from collections import defaultdict

import csv
import decimal
import hashlib
import heapq
import json
import re
import os
import pprint
import sys
import time
import urllib.request, urllib.error, urllib.parse
import urllib.parse

try:
    import readline
except ImportError:
    pass

parser = argparse.ArgumentParser(description='Compute capital gains/losses.')

parser.add_argument('histories', metavar='FILE', nargs='+',
                   help='a csv or json file')

parser.add_argument('--ignore_old_coinbase', default='auto',
                    help='Ignore old coinbase files (in favor of api-downloaded ones).')

parser.add_argument('--fmv_url', dest='fmv_urls',
                    action='append',
                    default=[
                        'https://api.blockchain.info/charts/market-price?timespan=all&daysAverageString=1&format=csv',
                    ],
                    help='fair market value prices urls')

parser.add_argument('--data', dest='data', default='data.json',
                   help='external transaction info')

parser.add_argument('--transfer_window_hours', default=24)

parser.add_argument('--method', default='fifo', help='used to select which lot to sell; one of fifo, lifo, oldest, newest')

parser.add_argument("-y", "--non_interactive", help="don't prompt the user to confirm external transfer details",
                    action="store_true")

parser.add_argument("--consolidate_bitcoind", help="treat bitcoind accounts as one", action="store_true")

parser.add_argument("--consolidate_coinbase", help="treat coinbase accounts as one", action="store_true")

parser.add_argument("--external_transactions_file", default="external_transactions.json")

parser.add_argument("--flat_transactions_file", default="all_transactions.csv")

parser.add_argument("--nowash", default=False, action="store_true")

parser.add_argument("--buy_in_sell_month", default=False, action="store_true")

parser.add_argument("--cost_basis", default=False, action="store_true")

parser.add_argument("--end_date", metavar="YYYY-MM-DD")

parser.add_argument("--list_purchases", default=False, action="store_true")

parser.add_argument("--list_gifts", default=False, action="store_true")

class TransactionParser(object):
    counter = 0
    def can_parse(self, filename):
        # returns bool
        raise NotImplementedError
    def parse_file(self, filename):
        # returns list[Transaction]
        raise NotImplementedError
    def merge(self, transactions):
        # returns Transaction
        assert len(transactions) == 1
        return transactions[0]
    def merge_some(self, transactions):
        # returns list[Transaction]
        return [self.merge(transactions)]
    def default_account(self):
        return self.__class__.__name__.replace('Parser', '').rstrip('0123456789')
    def check_complete(self):
        pass
    def reset(self):
        pass
    def unique(self, timestamp):
        self.counter += 1
        return "%s:%s:%s" % (self.default_account(), timestamp, self.counter)

class BitcoindParser(TransactionParser):
    def can_parse(self, filename):
        # TODO: This is way to loose...
        start = re.sub(r'\s+', '', open(filename).read(100))
        # Old Bitcoin Core versions begin with "account" key; newer versions
        # begin with "address" key instead.
        return start.startswith('[{"account":') or start.startswith('[{"address":')
    def parse_file(self, filename):
        for item in json.load(open(filename)):
            timestamp = time.localtime(item['time'])
            item['amount'] = decimal.Decimal(item['amount']).quantize(decimal.Decimal('1e-8'))
            item['fee'] = decimal.Decimal(item.get('fee', 0)).quantize(decimal.Decimal('1e-8'))
            # Include the vout so that atomic payments don't get dropped.
            item['txid'] = item['txid'] + ":" + str(item['vout'])
            info = ' '.join([item.get('to', ''), item.get('comment', ''), item.get('address', '')])
            if not parsed_args.consolidate_bitcoind:
                account = ('bitcoind-%s' % item['account']).strip('-')
            else:
                account = 'bitcoind'
            confirmations = item['confirmations']
            # Negative confirmations indicate a conflicted transaction.
            if confirmations < 0:
                continue
            if item['category'] == 'receive':
                yield Transaction(timestamp, 'deposit', item['amount'], 0, 0, id=item['txid'], info=info, account=account)
            elif item['category'] == 'generate':
                yield Transaction(timestamp, 'deposit', item['amount'], 0, 0, id=item['txid'], info=info, account=account)
            elif item['category'] == 'send':
                yield Transaction(timestamp, 'withdraw', item['amount'], 0, 0, fee_btc=item.get('fee', 0), id=item['txid'], info=info, account=account)
            elif item['category'] == 'move' and item['amount'] < 0 and not parsed_args.consolidate_bitcoind:
                t = Transaction(timestamp, 'transfer', item['amount'], 0, 0, info=info, account=account)
                t.dest_account = ('bitcoind-%s' % item['otheraccount']).strip('-')
                yield t
    def merge_some(self, transactions):
        # don't double-count the fee
        for t in transactions[1:]:
            t.fee_btc = 0
        return transactions

class RawBitcoinInfoParser(TransactionParser):
    @staticmethod
    def fee(txn):
        return (sum(input['prev_out']['value'] for input in txn['inputs'])
                        - sum(output['value'] for output in txn['out'])) / satoshi_to_btc
    @staticmethod
    def is_withdrawal(txn, addresses):
        if any(input['prev_out']['addr'] in addresses for input in txn['inputs']):
            if not all(input['prev_out']['addr'] in addresses for input in txn['inputs']):
                raise NotImplementedError("Sends from mixed controlled and not controlled addresses %s" % txn)
            return True
        else:
            return False

class AddressListParser(RawBitcoinInfoParser):
    """Treat a set of public addresses as a single acount.

    Parses a simple text file with one public address per line, downloading the
    transaction history from blockchain.info.

    This is useful for any public wallet where you can enumerate addresses
    used but otherwise can't export transactions.  This includes many
    lightweight, mobile, and hardware wallets.
    """
    def can_parse(self, filename):
        for line in open(filename):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            elif re.match('^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$', line):
                return True
            else:
                return False
    def parse_file(self, filename):
        addresses = []
        for line in open(filename):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            else:
                addresses.append(line)
        txns = {}
        for address in addresses:
            for txn in json.load(
                open_cached('https://blockchain.info/address/%s?format=json' % address))['txs']:
                if txn['hash'] not in txns:
                    txns[txn['hash']] = txn
        for txn in txns.values():
            timestamp = time.localtime(txn['time'])
            if self.is_withdrawal(txn, addresses):
                fee = self.fee(txn)
                for ix, output in enumerate(txn['out']):
                    if output['addr'] not in addresses:
                        yield Transaction(timestamp, 'withdraw', -decimal.Decimal(output['value']) / satoshi_to_btc, 0, id="withdraw:%s:%s" % (txn['hash'], ix), account=filename, fee_btc=fee, txid=txn['hash'])
                        fee = 0
            else:
                for ix, output in enumerate(txn['out']):
                    if output['addr'] in addresses:
                        yield Transaction(timestamp, 'deposit', decimal.Decimal(output['value']) / satoshi_to_btc, 0, id="deposit:%s:%s" % (txn['hash'], ix), account=filename, txid=txn['hash'])
    def merge_some(self, transactions):
        return transactions

class BitcoinInfoParser(RawBitcoinInfoParser):
    # https://blockchain.info/address/ADDRESS?format=json
    def can_parse(self, filename):
        # TODO: This is way to loose...
        head = open(filename).read(200)
        return head[0] == '{' and '"n_tx":' in head and '"address":' in head
    def parse_file(self, filename):
        all = json.load(open(filename))
        address = all['address']
        for txn in all['txs']:
            timestamp = time.localtime(txn['time'])
            if self.is_withdrawal(txn, [address]):
                fee = self.fee(txn)
                for ix, output in enumerate(txn['out']):
                    yield Transaction(timestamp, 'withdraw', -decimal.Decimal(output['value']) / satoshi_to_btc, 0, id="withdraw:%s:%s" % (txn['hash'], ix), account=address, fee_btc=fee, txid=txn['hash'])
                    fee = 0
            else:
                for ix, output in enumerate(txn['out']):
                    if output['addr'] == address:
                        yield Transaction(timestamp, 'deposit', decimal.Decimal(output['value']) / satoshi_to_btc, 0, id="%s-%s:%s" % (address, txn['hash'], ix), account=address, txid=txn['hash'])
    def merge_some(self, transactions):
        return transactions


class CsvParser(TransactionParser):
    expected_header = None
    def can_parse(self, filename):
        return re.match(self.expected_header, open(filename).readline().strip())
    def parse_row(self, row):
        raise NotImplementedError
    def parse_file(self, filename):
        self.filename = filename
        self.start()
        first = True
        for ix, row in enumerate(csv.reader(open(filename))):
            if not row or first:
                first = False
                self.header = row
                continue
            elif row[0].startswith('#'):
                continue
            else:
                try:
                    transaction = self.parse_row(row)
                    if transaction is not None:
                        yield transaction
                except Exception:
                    print(ix, row)
                    raise
        for transaction in self.finish():
            yield transaction
    def start(self):
        pass
    def finish(self):
        return ()

class BitstampParser(CsvParser):
    expected_header = 'Type,Datetime,BTC,USD,BTC Price,FEE,Sub Type'

    def parse_row(self, row):
        type, timestamp, btc, usd, price, fee, _, _ = row
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        if type == '0':
            return Transaction(timestamp, 'deposit', btc, 0, 0)
        elif type == '1':
            return Transaction(timestamp, 'withdraw', btc, 0, 0)
        elif type == '2':
            return Transaction(timestamp, 'trade', btc, usd, price, fee)
        else:
            raise ValueError(type)

class BitstampParser2(CsvParser):
    expected_header = 'Type,Datetime,Account,Amount,Value,Rate,Fee,Sub Type'

    @staticmethod
    def _trim(s, suffix):
        if not s:
            return None
        else:
            assert s.endswith(suffix), (s, suffix)
            return s[:-len(suffix)]

    _last_timestamp = None
    def add_secs(self, timestamp):
      # Add seconds to preserve order in file, as granularity is only minutes.
      if timestamp == self._last_timestamp:
        self._secs += 1
        return time.localtime(time.mktime(timestamp) + self._secs)
      else:
        self._last_timestamp = timestamp
        self._secs = 0
        return timestamp

    def parse_row(self, row):
        type, timestamp, _, btc, usd, price, fee, buy_sell = row

        timestamp = self.add_secs(time.strptime(timestamp, '%b. %d, %Y, %I:%M %p'))
        btc = self._trim(btc, ' BTC')
        usd = self._trim(usd, ' USD')
        price = self._trim(price, ' USD')
        fee = self._trim(fee, ' USD') or 0

        if type == 'Deposit':
            return Transaction(timestamp, 'deposit', btc, 0, 0)
        elif type == 'Withdrawal':
            return Transaction(timestamp, 'withdraw', '-' + btc, 0, 0)
        else:
            if buy_sell == 'Sell':
              btc = '-' + btc
            else:
              usd = '-' + usd
            return Transaction(timestamp, 'trade', btc, usd, price, fee)

class TransactionParser(CsvParser):
    expected_header = 'timestamp,account,type,btc,usd,fee_btc,fee_usd,info'

    def parse_row(self, row):
        if not row[0] or row[0][0] == '#':
            return None
        timestamp,account,type,btc,usd,fee_btc,fee_usd,info = row
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        return Transaction(timestamp, type, btc, usd, fee_btc=fee_btc, fee_usd=fee_usd, account=account, info=info)

class ElectrumParser(CsvParser):
    # expected_header = 'transaction_hash,label,confirmations,value,fee,balance,timestamp'
    electrum_version = 0

    def can_parse(self, filename):
        first_line = open(filename).readline().strip()
        if re.match ('transaction_hash,label,confirmations,value,timestamp', first_line): # Electrum 2.5.4
            self.electrum_version = 2
        elif re.match ('transaction_hash,label,confirmations,value,fiat_value,fee,fiat_fee,timestamp', first_line): # Electrum 3.x
            self.electrum_version = 3
        else:
            return False
        return True

    def parse_row(self, row):
        if self.electrum_version == 2:
            transaction_hash,label,confirmations,value,timestamp = row
            fee = tx_fee(transaction_hash)
            # TODO: Why isn't this exported anymore?
            timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M')
        elif self.electrum_version == 3:
            transaction_hash,label,confirmations,value,fiat_value,fee,fiat_fee,timestamp = row
            fee = fee[:-4] #Remove the " BTC"
            timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            value = value[:-4] #Remove the " BTC" at the end
            if value[-1] == ".":
                value = value[:-1] #remove decimal point if nothing follows it. Not doing so confuses python.
            if value[0] != "-": #So it's a positive... add a "+" to make it like the electrum 2 string
                value = "+" + value
        else:
            raise ValueError("Electrum parser: Unknown format")


        timestamp = time.localtime(time.mktime(timestamp) + 7*60*60)
        if not label:
            label = 'unknown'
        elif label[0] == '<' and label[-1] != '>':
            label = label[1:]
        common = dict(usd=None, info=label, id=transaction_hash, txid=transaction_hash)
        if value[0] == '+':
            return Transaction(timestamp, 'deposit', value[1:], **common)
        else:
            assert value[0] == '-'
            true_value = decimal.Decimal(value) + decimal.Decimal(fee)
            if true_value == 0:
                return Transaction(timestamp, 'fee', fee, **common)
            else:
                return Transaction(timestamp, 'withdraw', true_value, fee_btc=fee, **common)

    def merge_some(self, transactions):
        return transactions


class NewCoinbaseParser(CsvParser):
    def can_parse(self, filename):
        first_line = open(filename).readline().strip()
        if 'You can use this transaction report to inform your likely tax obligations' in first_line and 'Coinbase' in first_line:
            if not parsed_args.consolidate_coinbase:
                raise RuntimeError(
                    'New-style coinbase transaction are missing wallet designations; '
                    'please use download-coinbase.py '
                    'or pass --consolidate_coinbase to treat all Coinbase wallets as one.'
                    'WARNING: This report is lacks all transfers to/from Coinbase Pro, '
                    'and will give incorrect results if there are any such transctions.')
            else:
                return True

    def reset(self):
        self.started = False

    def parse_row(self, row):
        if not self.started:
            if row[0] == 'Timestamp':
                self.header = row
                self.started = True
            return None

        data = dict(zip(self.header, row))
        if data['Asset'] != 'BTC':
            return None
        timestamp = time.strptime(data['Timestamp'], '%Y-%m-%dT%H:%M:%SZ')
        type = data['Transaction Type'].lower()
        if type in ('paid for an order', 'send'):
            type = 'withdraw'
        elif type in ('receive',):
            type = 'deposit'
        elif type not in ('buy', 'sell'):
            raise ValueError('Unknown type of transaction: %s' % type)

        btc = data['Quantity Transacted']
        usd = data['USD Subtotal'] or 0
        if not usd:
            usd = decimal.Decimal(data['USD Spot Price at Transaction']) * decimal.Decimal(btc)

        if type in ('buy',):
            usd = '-%s' % usd
        if type in ('sell', 'withdraw'):
            btc = '-%s' % btc
        if type in ('buy', 'sell'):
            type = 'trade'

        info = data.get('Notes')
        if type == 'withdraw':
            if data['Transaction Type'] != 'Paid for an order':
                print('Warning: bitcoin fees not specified for Coinbase %s, may not match external transaction.' % info)
            fee_btc = 0
        else:
            fee_btc = 0
        fee_usd = data['USD Fees'] or 0
        account = 'Coinbase:consolidated'

        return Transaction(timestamp, type, btc, usd, fee_btc=fee_btc, fee_usd=fee_usd, account=account, info=info)


class DownloadedCoinbaseParser(TransactionParser):
    expected_header = '# Coinbase downloaded transactions .*'
    def can_parse(self, filename):
        return re.match(self.expected_header, open(filename).readline().strip())
    def parse_file(self, filename):
        with open(filename) as fin:
            fin.readline()
            data = json.load(fin)
        if parsed_args.consolidate_coinbase:
            account = 'Coinbase:consolidated'
        else:
            account = 'Coinbase:%s:%s' % (data['account']['name'], data['account']['id'])
        for transaction in data['transactions'].values():
            date, hour = transaction['created_at'].split('Z')[0].split('T')
            timestamp = time.strptime(date + " " + hour, '%Y-%m-%d %H:%M:%S')

            assert transaction['amount']['currency'] == 'BTC'
            btc = transaction['amount']['amount']
            if 'native_amount' in transaction and transaction['native_amount']['currency'] == 'USD':
                usd = transaction['native_amount']['amount']
            if 'network' in transaction:
                txid = transaction['network'].get('hash')
                fee_btc = transaction['network'].get('transaction_fee')
            else:
                txid = None
                fee_btc = None

            if transaction['type'] == 'buy':
                type = 'trade'
                usd = '-' + usd
            elif transaction['type'] == 'sell':
                type = 'trade'
                usd = '-' + usd
            elif transaction['type'] in ('send', 'pro_deposit', 'pro_withdrawal', 'vault_deposit', 'vault_withdrawal', 'order', 'exchange_deposit', 'exchange_withdrawal', 'transfer'):
                if btc[0] == '-':
                    type = 'withdraw'
                else:
                    type = 'deposit'
            else:
                raise ValueError('Unknown type: %s' % transaction['type'])

            yield Transaction(
                id=transaction['id'],
                account=account,
                timestamp=timestamp,
                btc=btc,
                usd=usd,
                type=type,
                info=transaction['details']['title'] + ' ' + transaction['details']['subtitle'],
                txid=txid)


class CoinbaseParser(CsvParser):
    expected_header = r'(User,.*,[0-9a-f]+)|(^Transactions$)'
    started = False

    def reset(self):
        self.account = None
        self.started = False

    def parse_file(self, filename):
        if parsed_args.ignore_old_coinbase == 'auto':
            # Actually reset the argument and fall through to do it just once.
            new_coinbase_parser = DownloadedCoinbaseParser()
            for path in parsed_args.histories:
                if new_coinbase_parser.can_parse(path):
                    parsed_args.ignore_old_coinbase = 'true'
                    break
            else:
                parsed_args.ignore_old_coinbase = 'false'
        if parsed_args.ignore_old_coinbase.lower() in ('true', 'yes'):
            ignore_old_coinbase = True
        elif parsed_args.ignore_old_coinbase.lower() in ('false', 'no'):
            ignore_old_coinbase = True
        else:
            raise ValueError('Unknown value for ignore_old_coinbase: %s' % parsed_args.ignore_old_coinbase)
        if not ignore_old_coinbase:
            yield from super(CoinbaseParser, self).parse_file(filename)

    def parse_row(self, row):
        if not self.started:
            if row[0] == 'Account':
                self.account = 'Coinbase:%s:%s' % (row[1], row[2])
            raw_row = ",".join(row)
            if (raw_row.startswith('Timestamp,Balance,')
                or raw_row.startswith('User,')
                or raw_row.startswith('Account,')):
                # Coinbase has multiple header lines.
                return None
            self.started = True
        timestamp, _, btc, _, to, note, _, total, total_currency = row[:9]
        date, hour, zone = timestamp.split()
        timestamp = time.strptime(date + " " + hour, '%Y-%m-%d %H:%M:%S')
        offset = int(zone[:-2]) * 3600 + int(zone[-2:]) * 60
        timestamp = time.localtime(time.mktime(timestamp) - offset)
        if 'will arrive in your bank account' in note:
          note = '$' + note
        if '$' in note:
            # It's a buy/sell
            if total:
                assert total_currency == 'USD'
                usd = total
            else:
                prices = re.findall(r'\$\d+\.\d+', note)
                if len(prices) != 1:
                    raise ValueError("Ambiguous or missing price: %s" % note)
                usd = prices[0][1:]
            type = 'trade'
            if 'Paid for' in note or 'Bought' in note:
                usd = '-' + usd
        else:
            usd = 0
            type = 'deposit' if float(btc) > 0 else 'withdraw'
        info = " ".join([note, to])
        if parsed_args.consolidate_coinbase:
            account = 'Coinbase:consolidated'
        elif True:
            account = self.account or self.filename
        else:
            account = None
        if re.match('[0-9a-f]{60,64}', row[-1]):
          txid = row[-1]
        else:
          txid = None
        return Transaction(timestamp, type, btc, usd, info=info, account=account, txid=txid)


# AKA Coinbase Pro
class GdaxParser(CsvParser):

    def parse_time(self, stime):
        return time.strptime(stime.replace('Z', '000'), '%Y-%m-%dT%H:%M:%S.%f')

    def default_account(self):
        return 'CoinbasePro'

class GdaxFillsParser(GdaxParser):
    expected_header = '(portfolio,)?trade id,product,side,created at,size,size unit,price,fee,total,price/fee/total unit'

    def parse_row(self, row):
        if self.header[0] == 'portfolio':
            account = row.pop(0)
            if account == 'default':
                account = self.default_account()
        else:
            account = self.default_account()
        trade, product, buy_sell, stime, btc, unit, price, fee_usd, total, pft_unit = row
        if product != 'BTC-USD':
            return None
        assert unit == 'BTC' and pft_unit == 'USD'
        timestamp = self.parse_time(stime)
        usd = decimal.Decimal(total) + decimal.Decimal(fee_usd)
        if buy_sell == 'BUY':
          return Transaction(timestamp, 'trade', btc, usd, fee_usd=fee_usd, account=account)
        elif buy_sell == 'SELL':
          return Transaction(timestamp, 'trade', '-' + btc, usd, fee_usd=fee_usd, account=account)
        else:
            raise ValueError("Unknown transactiont type: %s" % buy_sell)


class GdaxAccountParser(GdaxParser):
    expected_header = '(portfolio,)?type,time,amount,balance,amount/balance unit,transfer id,trade id,order id'

    def parse_row(self, row):
        if self.header[0] == 'portfolio':
            account = row.pop(0)
            if account == 'default':
                account = self.default_account()
        else:
            account = self.default_account()
        type, stime, amount, _, unit, tid, _, _ = row
        if unit != 'BTC':
            # Ignore non-BTC withdrawals and deposits.
            return None
        timestamp = self.parse_time(stime)
        if type == 'match':
          return None  # handled in fills
        elif type == 'deposit':
          return Transaction(timestamp, 'deposit', amount, 0, id=tid, account=account)
        elif type == 'withdrawal':
          return Transaction(timestamp, 'withdraw', amount, 0, id=tid, account=account)
        else:
            raise ValueError("Unknown transactiont type: %s" % type)

class KrakenParser(CsvParser):

    def start(self):
        self._trades = defaultdict(dict)

    def can_parse(self, filename):
        first_line = open(filename).readline().strip()
        if first_line.endswith('"ledgers"'):
            raise ValueError("Use ledger, not trade, export for Kraken.")
        elif first_line == '"txid","refid","time","type","subtype","aclass","asset","amount","fee","balance"':
            return True

    def parse_row(self, row):
        txid, refid, ktimestamp, ktype, _, _, asset, amount, fee, _ = row
        timestamp = time.strptime(ktimestamp, '%Y-%m-%d %H:%M:%S')
        if ktype == 'trade':
            info = self._trades[refid]
            assert asset not in info
            info[asset] = row
            if len(info) >= 3 and 'XXBT' in info:
                btc = info['XXBT'][6]
                if 'ZUSD' in info:
                    usd = info['ZUSD'][6]
                    type = 'trade'
                else:
                    usd = 0
                    type = 'deposit' if float(btc) > 0 else 'withdraw'
                del self._trades[refid]
                return Transaction(timestamp, type, btc, usd)
            else:
                return
        elif asset != 'XXBT':
            return None

        if ktype == 'deposit':
            return Transaction(timestamp, ktype, amount, 0)
        elif ktype == 'withdrawal':
            return Transaction(timestamp, 'withdraw', amount, 0, fee_btc=fee)
        else:
            raise NotImplementedError(ktype + ': ' + ','.join(row))

    def finish(self):
        if self._trades:
            unfinished = [(refid, info) for refid, info in self._trades
                          if len(info) < (2 + 'KFEE' in info)]
            if unfinished:
                pprint.pprint(dict(unfinished))
                raise ValueError('Unfinished trades.')
        return ()


class MtGoxParser(CsvParser):
    expected_header = 'Index,Date,Type,Info,Value,Balance'

    def __init__(self):
        self.seen_file_count = [0, 0]
        self.seen_transactions = [set(), set()]

    def parse_file(self, filename):
        basename = os.path.basename(filename).upper()
        if 'BTC' in basename:
            self.is_btc = True
        elif 'USD' in basename:
            self.is_btc = False
        else:
            raise ValueError("mtgox must contain BTC or USD")
        self.seen_file_count[self.is_btc] += 1
        for t in CsvParser.parse_file(self, filename):
            yield t

    def parse_row(self, row):
        ix, timestamp, type, info, value, balance = row
        ix = int(ix)
        if ix in self.seen_transactions[self.is_btc]:
            raise ValueError("Duplicate tranaction: %s" % ix)
        else:
            self.seen_transactions[self.is_btc].add(ix)
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        value = decimal.Decimal(value)
        m = re.search(r'tid:\d+', info)
        if m:
            id = "MtGox:%s" % m.group(0)
        else:
            id = "MtGox[%s]:%s" % (('UDSD', 'BTC')[self.is_btc], ix)
        if type == 'out':
            return Transaction(timestamp, 'trade', -value, 0, 0, 0, info=info, id=id)
        elif type == 'in':
            return Transaction(timestamp, 'trade', value, 0, 0, 0, info=info, id=id)
        elif type == 'earned':
            return Transaction(timestamp, 'trade', 0, value, 0, 0, info=info, id=id)
        elif type == 'spent':
            return Transaction(timestamp, 'trade', 0, -value, 0, 0, info=info, id=id)
        elif type == 'fee':
            if self.is_btc:
                return Transaction(timestamp, 'fee', 0, 0, 0, fee_btc=value, info=info, id=id)
            else:
                return Transaction(timestamp, 'fee', 0, 0, 0, fee_usd=value, info=info, id=id)
        elif type == 'withdraw' and self.is_btc:
            return Transaction(timestamp, 'withdraw', -value, 0, 0, 0, info=info, id=id)
        elif type == 'deposit' and self.is_btc:
            return Transaction(timestamp, 'deposit', value, 0, 0, 0, info=info, id=id)
        else:
            raise ValueError(type)

    def merge(self, transactions):
        if len(transactions) == 1:
            return transactions[0]
        types = set(t.type for t in transactions)
        if 'fee' in types:
            types.remove('fee')
        merged = Transaction(transactions[0].timestamp, list(types)[0], None, None, None, id=transactions[0].id)
        merged.parser = transactions[0].parser
        for t in transactions:
            for attr in ('account', 'btc', 'usd', 'fee_usd', 'fee_btc', 'price'):
                if getattr(t, attr):
                    setattr(merged, attr, getattr(t, attr))
        try:
            if not merged.price and transactions[0].type == 'trade':
                merged.price = merged.usd / merged.btc
            if not merged.fee_usd and merged.fee_btc:
                if merged.price:
                    merged.fee_usd = roundd(merged.price * merged.fee_btc, 4)
                else:
                    merged.btc += merged.fee_btc
        except Exception:
            print(len(transactions))
            for t in transactions:
                print(t, t.line)
            print(merged.__dict__)
            raise
        return merged

    def check_complete(self):
        if self.seen_file_count[0] != self.seen_file_count[1]:
            raise ValueError("Missmatched number of BTC and USD files (%s vs %s)." % tuple(seen_file_count))
        if self.seen_file_count[0] == self.seen_file_count[1] == 0:
            return
        usd_or_btc = ['USD', 'BTC']
        for is_btc in (True, False):
            transactions = self.seen_transactions[is_btc]
            if len(transactions) == 0:
                pass
            elif len(transactions) != max(transactions):
                for gap_start in range(1, len(transactions)):
                    if gap_start not in transactions:
                        break
                for gap_end in range(gap_start, max(transactions)):
                    if gap_end in transactions:
                        break
                raise ValueError("Missing transactions in mtgox %s history (%s to %s)." % (usd_or_btc[is_btc], gap_start, gap_end-1))

class DbDumpParser(TransactionParser):
    # python bitcointools/dbdump.py --wallet-tx
    # NOTE: by default dbdump.py only prints 5 digits, fix to print the last satoshi
    # TODO(robertwb): Figure out how to extract the individual accounts.
    def can_parse(self, filename):
        return filename.endswith('.walletdump')
    def parse_file(self, filename):
        def parse_pseudo_dict(line):
            d = {}
            for item in line.replace(': ', ':').split():
                if ':' in item:
                    key, value = item.split(':', 1)
                    if key == 'value':
                        value = decimal.Decimal(value)
                    d[key] = value
            return d

        partial = False
        for line in open(filename):
            line = line.strip()
            if line.startswith('==WalletTransaction=='):
                assert not partial
                tx_id = line.split('=')[-1].strip()
                partial = True
                in_tx = []
                out_tx = []
                from_me = None
            elif line.startswith('TxIn:'):
                d = parse_pseudo_dict(line[5:])
                in_tx.append(d)
            elif line.startswith('TxOut:'):
                d = parse_pseudo_dict(line[6:])
                out_tx.append(d)
            elif line.startswith('mapValue:'):
                map_value = parse_pseudo_dict(line[10:-1].replace("'", "").replace(',', ' '))
            elif 'fromMe' in line:
#                from_me = 'fromMe:True' in line
                from_me = 'pubkey' not in in_tx[0]
                partial = False
                info = ' '.join(s for s in [map_value.get('to'), map_value.get('comment')] if s)
                timestamp = time.localtime(int(map_value['timesmart']))

                if from_me:
                    total_in = sum(tx['value'] for tx in in_tx)
                    total_out = sum(tx['value'] for tx in out_tx)
                    fee = total_in - total_out
                    for ix, tx in enumerate(out_tx):
                        if tx['Own'] == 'False':
                            yield Transaction(timestamp, 'withdraw', -tx['value'], 0, id="%s:%s" % (tx_id, ix), fee_btc=fee, info=info + ' ' + tx['pubkey'], account='wallet.dat')
                            fee = zero # only count the fee once
                    if fee:
                        yield Transaction(timestamp, 'fee', -fee, 0, id="%s:fee" % tx_id, info=info + ' fee', account='wallet.dat')
                else:
                    for ix, tx in enumerate(out_tx):
                        if tx['Own'] == 'True':
                            yield Transaction(timestamp, 'deposit', tx['value'], 0, id="%s:%s" % (tx_id, ix), info=info + ' ' + tx['pubkey'], account='wallet.dat')

        assert not partial

    def merge_some(self, transactions):
        return transactions


zero = decimal.Decimal('0', decimal.Context(8))
tenth = decimal.Decimal('0.1')
satoshi_to_btc = decimal.Decimal('1e8')
def roundd(x, digits):
    return x.quantize(tenth**digits)

def decimal_or_none(o):
    if isinstance(o, str) and o.startswith('--'):
        # Double negative.
        o = o[2:]
    return None if o is None else decimal.Decimal(o)

def strip_or_none(o):
    return o.strip() if o else o

class Transaction(object):
    def __init__(self, timestamp, type, btc, usd, price=None, fee_usd=0, fee_btc=0, info=None, id=None, account=None, parser=None, txid=None):
        self.timestamp = timestamp
        self.type = type
        self.btc = decimal_or_none(btc)
        self.usd = decimal_or_none(usd)
        self.price = decimal_or_none(price)
        self.fee_usd = decimal_or_none(fee_usd)
        self.fee_btc = decimal_or_none(fee_btc)
        self.info = strip_or_none(info)
        if self.btc and self.usd and self.price is None:
            self.price = self.usd / self.btc
        self.id = id
        self.account = account
        if parser:
            self.parser = parser
        self.txid = txid

    def __eq__(left, right):
        return left.timestamp == right.timestamp and left.btc == right.btc and left.id == right.id

    def __lt__(left, right):
        if left.timestamp == right.timestamp:
            # Prioritize transferring in to avoid negative balance.
            if left.type == 'transfer' and left.dest_account == right.account:
                return left.btc < 0
            elif right.type == 'transfer' and right.dest_account == left.account:
                return right.btc > 0
            else:
                return (right.btc, str(left.id)) < (left.btc, str(right.id))
        else:
            return left.timestamp < right.timestamp
#        return (left.timestamp, right.btc, str(left.id)) < (right.timestamp, left.btc, str(right.id))

    def __str__(self):
        if self.fee_btc:
            fee_str = ", fee=%s BTC" % self.fee_btc
        elif self.fee_usd:
            fee_str = ", fee=%s USD" % self.fee_usd
        else:
            fee_str = ""
        if self.type == 'transfer':
            dest_str = ', dest=%s' % self.dest_account
        else:
            dest_str = ""
        if self.txid:
            txid_str = ', txid=%s...' % self.txid[:6]
        else:
            txid_str = ''
        return "%s(%s, %s, %s, %s%s%s%s)" % (self.type, time.strftime('%Y-%m-%d %H:%M:%S', self.timestamp), self.usd, self.btc, self.account, fee_str, dest_str, txid_str)

    __repr__ = __str__

    @classmethod
    def csv_cols(cls):
        return ('time', 'type', 'usd', 'btc', 'price', 'fee_usd', 'fee_btc', 'account', 'id', 'info')
    @classmethod
    def csv_header(cls, sep=','):
        return sep.join(cls.csv_cols())
    def csv(self, sep=','):
        cols = []
        for col_name in self.csv_cols():
            if col_name == 'time':
                value = time.strftime('%Y-%m-%d %H:%M:%S', self.timestamp)
            else:
                value = str(getattr(self, col_name))
            cols.append(value)
        return sep.join(cols).replace('\n', ' ')

class Lot:
    def __init__(self, timestamp, btc, usd, transaction, dissallowed_loss=0):
        self.timestamp = timestamp
        self.btc = btc
        self.usd = usd
        self.price = usd / btc
        self.transaction = transaction
        self.dissallowed_loss = dissallowed_loss

    def split(self, btc):
        """
        Splits this lot into two, with the first consisting of at most btc bitcoins.
        """
        if btc <= 0:
            return None, self
        elif btc < self.btc:
            usd = roundd(self.price * btc, 2)
            dissallowed_loss = roundd(self.dissallowed_loss * btc / self.btc, 2)
            return (Lot(self.timestamp, btc, usd, self.transaction, dissallowed_loss),
                    Lot(self.timestamp, self.btc - btc, self.usd - usd, self.transaction, self.dissallowed_loss - dissallowed_loss))
        else:
            return self, None

    def __eq__(left, right):
        return left.timestamp == right.timestamp and left.transaction == right.transaction

    def __lt__(left, right):
        if parsed_args.method in ('fifo', 'oldest'):
            return (left.timestamp, left.transaction) < (right.timestamp, right.transaction)
        elif parsed_args.method in ('lifo', 'newest'):
            return (right.timestamp, left.transaction) < (left.timestamp, right.transaction)

    def __str__(self):
        dissallowed_loss = ", dissallowed_loss=%s" % self.dissallowed_loss if self.dissallowed_loss else ""
        return "Lot(%s, %s, %s%s)" % (time.strftime('%Y-%m-%d', self.timestamp), self.btc, self.price, dissallowed_loss)

    __repr__ = __str__

# Why is this not a class?
class Heap:
    def __init__(self, data=[]):
        self.data = list(data)
    def push(self, item):
        heapq.heappush(self.data, item)
    def pop(self):
        return heapq.heappop(self.data)
    def __len__(self):
        return len(self.data)


class LotSelector(object, metaclass=abc.ABCMeta):
    def __init__(self, data=[]):
        self._data = list(data)

    @abc.abstractmethod
    def push(self, lot):
        pass

    @abc.abstractmethod
    def pop(self):
        pass

    @abc.abstractmethod
    def unpop(self):
        pass

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        copy = type(self)(self._data)
        while len(copy):
            yield copy.pop()


class Fifo(LotSelector):
    def push(self, lot):
        self._data.append(lot)
    def pop(self):
        return self._data.pop(0)
    def unpop(self, lot):
        self._data.insert(0, lot)

class Lifo(LotSelector):
    def push(self, lot):
        self._data.append(lot)
    def pop(self):
        return self._data.pop()
    def unpop(self, lot):
        self._data.append(lot)

class HeapLotSelector(LotSelector):
    def push(self, lot):
        heapq.heappush(self._data, lot)
    def pop(self):
        return heapq.heappop(self._data)
    def unpop(self, lot):
        self.push(lot)

class OldestLotSelector(HeapLotSelector):
    def __init__(self, data=[]):
      # This impacts transaction sorting.
      assert parsed_args.method == 'oldest'
      super(OldestLotSelector, self).__init__(data)

class NewestLotSelector(HeapLotSelector):
    def __init__(self, data=[]):
      # This impacts transaction sorting.
      assert parsed_args.method == 'newest'
      super(NewestLotSelector, self).__init__(data)

def create_lot_selector():
    if parsed_args.method == 'fifo':
        return Fifo()
    elif parsed_args.method == 'lifo':
        return Lifo()
    elif parsed_args.method == 'oldest':
        return OldestLotSelector()
    elif parsed_args.method == 'newest':
        return NewestLotSelector()
    else:
        raise ValueError('Unknown lot selection type: "%s"' % parsed_args.method)


def url_to_filename(url):
    parts = urllib.parse.urlparse(url)
    hash = hashlib.sha1(url.encode('utf-8')).hexdigest()
    raw = '-'.join([parts.hostname] + parts.path.split('/') + parts.query.split('&'))
    return re.sub('[^a-zA-Z0-9_-]', '_', raw[:100]) + '-' + hash[:8]

already_forced_download = set()
def open_cached(url, force_download=False, cache_dir='download-cache', sleep=0):
    global already_forced_download
    if '://' not in url:
        # It's a (possibly relative) file path.
        return open(url)
    parts = urllib.parse.urlparse(url)
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    basename = os.path.join(cache_dir, url_to_filename(url))
    old_basename = 'cached-' + parts.hostname + '-' + parts.path.split('/')[-1]
    if os.path.exists(old_basename) and not os.path.exists(basename):
        os.rename(old_basename, basename)
    if not os.path.exists(basename) or (force_download and url not in already_forced_download):
        already_forced_download.add(url)
        time.sleep(sleep)
        request = urllib.request.Request(
            url=url,
            data=None,
            headers={'User-Agent': 'Mozilla/5.0 (%s)' % os.path.basename(__file__)})
        handle = urllib.request.urlopen(request)
        try:
            open(basename, 'wb').write(handle.read())
        except:
            return urllib.request.urlopen(url)
    return open(basename)

prices = {}
def fmv(timestamp):
    if timestamp is None:
        quote = json.load(urllib.request.urlopen('https://api.coindesk.com/v1/bpi/currentprice.json'))
        return round(decimal.Decimal(quote['bip']['USD']['rate']), 2)
    date = time.strftime('%Y-%m-%d', timestamp)
    if date not in prices:
        # For consistency, use previously fetched prices.
        fetch_prices(False)
    if date not in prices:
        fetch_price(date)
    return prices[date]

def fetch_price(date, force_download=False):
    if any('api.blockchain.info' in url for url in parsed_args.fmv_urls):
        fetch_price_blockchain(date, force_download=force_download)

    if date not in prices:
        # Fall back to coinmarketcap.
        fetch_price_coinmarketcap(date, force_download)

def fetch_price_blockchain(date, force_download=False):
    year = int(date.split('-')[0])
    historical_url = ('https://api.blockchain.info/charts/market-price'
                      '?start=%s-12-31&timespan=1year&daysAverageString=1&format=csv' % (year - 1))
    print(historical_url)
    for line in open_cached(historical_url, force_download=force_download, sleep=2):
        line = line.strip()
        if line:
            timestamp, price = line.split(',')
            prices[timestamp[:10]] = round(decimal.Decimal(price), 2)
    if date not in prices:
        if force_download:
            pass
        else:
            return fetch_price_blockchain(date, force_download=True)

def fetch_price_coinmarketcap(date, force_download=False):
    year = int(date.split('-')[0])
    historical_url = ('https://web-api.coinmarketcap.com/v1/cryptocurrency/'
                      'ohlcv/historical?symbol=BTC&convert=USD&time_start=%d-01-01&time_end=%d-12-31' % (year, year))
    print(historical_url)
    data = json.load(open_cached(historical_url, force_download=force_download, sleep=2))
    for quote in data['data']['quotes']:
        date = quote['time_open'][:10]
        low = quote['quote']['USD']['low']
        high = quote['quote']['USD']['high']
        prices[date] = round(decimal.Decimal((low + high) / 2), 2)
    if date not in prices:
        if force_download:
            pass
        else:
            return fetch_price_coinmarketcap(date, force_download=True)

def fetch_prices(force_download=False):
    print("Fetching fair market values...")
    for url in reversed(parsed_args.fmv_urls):
        if not url:
            # Empty parameter ignores all previous.
            break
        print(url)
        format = None
        for line in open_cached(url, force_download=force_download):
            line = line.strip()
            if not line:
                continue
            if format is None:
                if line.lower().replace('volume btc', 'volume') == 'datetime,high,low,average,volume':
                    format = 'bitcoinaverage'
                    continue
                elif re.match(r'\d\d/\d\d/\d\d\d\d \d\d:\d\d:\d\d,\d+\.\d*', line):
                    format = 'blockchain'
                elif re.match(r'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d,\d+\.\d*', line):
                    format = 'blockchain'
                else:
                    raise ValueError("Unknown format: %s" % line)
            cols = line.strip().split(',')
            if format == 'bitcoinaverage':
                date = cols[0].split()[0]
                if cols[1] and cols[2]:
                    price = (decimal.Decimal(cols[1]) + decimal.Decimal(cols[2])) / 2
                else:
                    price = cols[3]  # avg published for earlier dates
            else:
                if '-' in cols[0]:
                    date = cols[0].split()[0]
                else:
                    date = '-'.join(reversed(cols[0].split()[0].split('/')))
                price = cols[1]
            if date not in prices:
                prices[date] = decimal.Decimal(price)
    print("Done")

tx_fees = {}
def tx_fee(tx_hash):
    global tx_fees
    tx_fee_file = 'tx_fees.json'
    if not tx_fees and os.path.exists(tx_fee_file):
        tx_fees = json.load(open(tx_fee_file))
    if tx_hash in tx_fees:
        return decimal.Decimal(tx_fees[tx_hash])
    else:
        print("Downloading fee for tx", tx_hash)
        fee = decimal.Decimal(open_cached("https://blockchain.info/q/txfee/" + tx_hash).read().strip()) * decimal.Decimal('1e-8')
        tx_fees[tx_hash] = str(fee)
        json.dump(tx_fees, open(tx_fee_file, 'w'), indent=4)
        return fee

def is_long_term(buy, sell):
    # Years vary in length, making this a bit messy...
    def parts(t):
        return [int(x) for x in time.strftime('%Y %m %d %H %M %S', t).split(' ')]
    def plus_one_year(parts):
        return [parts[0] + 1] + parts[1:]
    return plus_one_year(parts(buy.timestamp)) < parts(sell.timestamp)


class RunningReport:
    def __init__(self, date_format):
        self.date_format = date_format
        self.data = {}
    def record(self, timestamp, **values):
        self.data[time.strftime(self.date_format, timestamp)] = values
    def dump(self, format):
        for date, diff in sorted(self.deltas().items()):
            print(format.format(date=date, **diff))
    def deltas(self):
        all = {}
        last = {}
        for date in sorted(self.data):
            data = self.data[date]
            diff = dict((key, value-last.get(key, 0)) for key, value in data.items())
            all[date] = diff
            last = data
        return all
    def consolidate(self, date_format):
        report = RunningReport(date_format)
        for date, values in sorted(self.data.items()):
            report.record(time.strptime(date, self.date_format), **values)
        return report

def re_input(prompt, regex, flags, default):
    if parsed_args.non_interactive:
        return default
    r = None
    while r is None or not re.match(regex, r, flags):
        r = input(prompt)
        if r == '':
            return default
        elif r == '?':
            return default + '?'
    return r

def option_input(prompt, options, default=None):
    regex = '|'.join(r"%s(%s)?\??" % (option[0], option[1:]) for option in options)
    if default != None:
        prompt += '[%s] ' % default.upper()[0]
        regex += r'|\?'
    res = re_input(prompt, regex, re.I, default=default)
    if res.endswith('?'):
        record = False
        res = res.strip('?')
    else:
        record = not parsed_args.non_interactive
    for option in options:
        if option.upper().startswith(res.upper()):
            return option, record

def value_input(prompt, btc, price):
    usd = roundd(btc * price, 2)
    value = re_input("%s [$%s or @%s] " % (prompt, usd, price), r"@\d+(.\d+)?|\$\d+(\.\d+)?", re.I, "@%s" % price)
    if value[0] == '@':
        price = decimal.Decimal(value[1:])
        usd = roundd(btc * price, 2)
    else:
        assert value[0] == '$'
        usd = decimal.Decimal(value[1:])
        price = usd / btc
    return usd, price

class FuzzyDict(object):
    def __init__(self, actual, alias_fn):
        self._actual = actual
        self._aliases = {}
        self._alias_fn = alias_fn
        for key in self._actual:
            alias = alias_fn(key)
            if alias in self._aliases:
                # Avoid ambiguity.
                self._aliases[alias] = None
            else:
                self._aliases[alias] = key
    def __contains__(self, key):
        return key in self._actual or self._aliases.get(self._alias_fn(key)) is not None
    def __getitem__(self, key):
        if key in self._actual:
            return self._actual[key]
        else:
            return self._actual[self._aliases.get(self._alias_fn(key))]
    def __setitem__(self, key, value):
        self._actual[key] = value

def load_external():
    if os.path.exists(parsed_args.external_transactions_file):
        actual = json.load(open(parsed_args.external_transactions_file))
    else:
        actual = {}
    return FuzzyDict(actual, short_id)

def save_external(external):
    if not parsed_args.non_interactive:
        json.dump(external._actual, open(parsed_args.external_transactions_file, 'w'), indent=4, sort_keys=True)

def short_id(id):
  return id.rsplit(':', 1)[0]


def parse_all(args):

    if args.end_date:
        max_timestamp = time.strptime(args.end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    else:
        max_timestamp = float('inf'),

    parsers = [
      BitstampParser(),
      BitstampParser2(),
      MtGoxParser(),
      BitcoindParser(),
      CoinbaseParser(),
      NewCoinbaseParser(),
      DownloadedCoinbaseParser(),
      GdaxAccountParser(),
      GdaxFillsParser(),
      ElectrumParser(),
      DbDumpParser(),
      AddressListParser(),
      BitcoinInfoParser(),
      TransactionParser(),
      KrakenParser(),
    ]
    all = []
    for file in args.histories:
        if '/ignore/' in file:
            continue
        for parser in parsers:
            if parser.can_parse(file):
                print(file, parser)
                parser.reset()
                for transaction in parser.parse_file(file):
                    transaction.parser = parser
                    if transaction.id is None:
                        transaction.id = parser.unique(transaction.timestamp)
                    if transaction.account is None:
                        transaction.account = parser.default_account()
                    all.append(transaction)
                parser.reset()
                break
        else:
            raise RuntimeError("No parser for " + file)
    for parser in parsers:
        parser.check_complete()

    by_date = defaultdict(list)
    for t in all:
        by_date[t.parser, t.id].append(t)
    for key, value in by_date.items():
        by_date[key] = key[0].merge_some(value)
    all = [t for merged in by_date.values() for t in merged if t.timestamp <= max_timestamp]
    all.sort()

    if args.flat_transactions_file:
        handle = open(args.flat_transactions_file, 'w')
        handle.write(Transaction.csv_header())
        handle.write('\n')
        for t in all:
            handle.write(t.csv())
            handle.write('\n')
        handle.close()

    return all


def match_transactions(all, args):
    def replace_with_transfer(withdrawal, deposit, **transaction_kwargs):
        transfer = Transaction(withdrawal.timestamp, 'transfer', withdrawal.btc, 0, **transaction_kwargs)
        transfer.account = withdrawal.account
        transfer.dest_account = deposit.account
        print("detected transfer: %s + %s -> %s" % (withdrawal, deposit, transfer))
        all.remove(withdrawal)
        all.remove(deposit)
        all.append(transfer)

    # First try to match transfers on amounts.
    deposits = defaultdict(list)
    for t in all:
        if t.type == 'deposit' and t.btc:
            deposits[t.btc].append(t)
#    pprint.pprint(deposits.items())

    for t in list(all):
        if t.type == 'withdraw' and t.btc:
            matches = deposits.get(-t.btc, ())
            for candidate in matches:
                if (abs(time.mktime(candidate.timestamp) - time.mktime(t.timestamp)) < args.transfer_window_hours * 3600
                    and t.account != candidate.account):
                    matches.remove(candidate)
                    replace_with_transfer(t, candidate, fee_btc=t.fee_btc, fee_usd=t.fee_usd)
                    break
            else:
                if matches:
                    print("no match on amount", t, matches)

    # Next try to match based on txids.  This could come first, but would
    # run into complications if transaction histories have been massaged to
    # work before issue #5 was resolved or transactions spanned multiple
    # withdrawals and deposits.

    deposits = defaultdict(list)
    for t in all:
        if t.type == 'deposit' and t.txid:
            deposits[t.txid].append(t)
#    pprint.pprint(deposits.items())

    for t in list(all):
        if t.type == 'withdraw' and t.btc:
            matches = deposits.get(t.txid, ())
            if len(matches) == 1:
                candidate, = matches
                matches.remove(candidate)
                fee = -(t.btc + candidate.btc)
                t.btc += fee
                replace_with_transfer(t, candidate, fee_btc=fee, txid=t.txid)
            elif matches:
                print("multiple matches", t, matches)


    pprint.pprint(sorted([(key, value) for key, value in deposits.items() if value],
                         key=lambda kv: kv[1][0].timestamp))

    return all


def main(args):
    all = match_transactions(parse_all(args), args)

    if args.end_date:
        max_timestamp = time.strptime(args.end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    else:
        max_timestamp = float('inf'),

    all.sort()
    for t in all:
        if t.type not in ('trade', 'transfer'):
            print(t)

    total_cost = 0
    account_btc = defaultdict(int)
    income = 0
    income_txn = []
    gross_receipts = 0
    gross_receipts_txn = []
    gift_txns = []
    gains = 0
    long_term_gains = 0
    long_term_gifts = 0
    total_buy = 0
    total_sell = 0
    total_cost_basis = 0
    long_term_cost_basis = 0
    long_term_gift_cost_basis = 0
    recent_sells = []
    dissallowed_loss = 0
    exit = False

    # TODO(robertwb): Make an Account class
    def push_lot(account, lot):
        to_sell, to_hold = lot.split(-account_btc[account])
        if to_hold:
            lots[account].push(to_hold)
        if to_sell:
            # cover short
            return -to_sell.usd
        else:
            return 0
    external = load_external()
    # Dict of accounts to lots.
    lots = defaultdict(create_lot_selector)
    all.sort()
    by_month = RunningReport("%Y-%m")
    transfered_out = []
    print()
    for ix, t in enumerate(all):
        if exit:
            break
        print(ix, t)
        timestamp = t.timestamp
        if timestamp > max_timestamp:
            break
        if t.type == 'trade':
            usd, btc = t.usd - t.fee_usd, t.btc
        elif t.type == 'transfer':
            usd, btc = 0, t.btc
        else:
            btc = t.btc
            if t.id in external:
                data = external[t.id]
                usd, price = decimal.Decimal(data['usd']), decimal.Decimal(data['price'])
                purchase_date = time.strptime(data['purchase_date'], '%Y-%m-%d %H:%M:%S')
                if data['type'] in ('transfer_out'):
                    t.type = 'transfer_out'
                elif data['type'] in ('income', 'expense'):
                    income_txn.append((time.strftime('%Y-%m-%d', t.timestamp), -usd))
                    income -= usd
                    if data['type'] == 'income':
                        gross_receipts_txn.append((time.strftime('%Y-%m-%d', t.timestamp), -usd))
                        gross_receipts -= usd
                elif data['type'] == 'gift':
                    t.type = 'gift'
                elif data['type'] in ('buy', 'sale', 'purchase'):
                    t.type = 'trade'
            else:
                price = t.price or fmv(t.timestamp)
                approx_usd = roundd(-price * btc, 2)
                print()
                print("On %s you %s %s btc (~$%s at %s/btc)." % (
                    time.strftime("%a, %d %b %Y %H:%M:%S +0000", t.timestamp),
                    ['sent', 'recieved'][t.btc > 0],
                    abs(t.btc),
                    abs(approx_usd),
                    price))
                print(t.info)
                save_choice = not args.non_interactive
                if btc == 0:
                    continue
                elif btc > 0:
                    type, save_choice = option_input("Is this Income, Transfer or a Buy: ", ['income', 'transfer', 'buy', 'abort', 'quit'], default='income')
                    if type in ('quit', 'abort'):
                        if type == 'quit':
                            save_external(external)
                        sys.exit(1)
                    elif type == 'transfer':
                        date = re_input("Purchase date: [%s]" % time.strftime('%Y-%m-%d', t.timestamp), r"\d\d-\d\d-\d\d\d\d", 0, default='')
                        if date:
                            timestamp = time.strptime(date, '%Y-%m-%d')
                        else:
                            timestamp = t.timestamp
                        usd, price = value_input("Cost basis: ", btc, price)
                        usd = -usd
                    else:
                        usd, price = value_input("How much was this worth in USD? ", btc, price)
                        usd = -usd
                        if type == 'income':
                            income_txn.append((time.strftime('%Y-%m-%d', t.timestamp), -usd))
                            income -= usd
                            gross_receipts_txn.append((time.strftime('%Y-%m-%d', t.timestamp), -usd))
                            gross_receipts -= usd
                else:
                    if t.type == 'fee':
                        type = 'fee'
                        usd = approx_usd
                    else:
                        type, save_choice = option_input("Is this a Sale, Purchase, Expense, Transfer, or (Charitable) Gift: ", ['sale', 'purchase', 'transfer', 'expense', 'gift', 'abort', 'quit'], default='purchase')
                        if type in ('quit', 'abort'):
                            if type == 'quit':
                                save_external(external)
                            sys.exit(1)
                        elif type == 'transfer':
                            # Mutate!
                            type = t.type = 'transfer_out'
                            usd = 0
                        else:
                            usd, price = value_input("How much was this worth in USD? ", abs(btc), price)
                            if type == 'expense':
                                income_txn.append((time.strftime('%Y-%m-%d', t.timestamp), -usd))
                                income -= usd
                if type != 'fee' and save_choice:
                    note = input('Note: ')
                    external[t.id] = { 'usd': str(usd), 'btc': str(btc), 'price': str(price),
                                       'type': type, 'note': note, 'info': t.info, 'account': t.account,
                                       'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', t.timestamp),
                                       'purchase_date': time.strftime('%Y-%m-%d %H:%M:%S', timestamp) }


        print(t)
        if btc < 0:
            btc -= t.fee_btc
        account_btc[t.account] += btc
        print("btc", btc, "usd", usd)
        if btc == 0:
            continue
        elif btc > 0:
            buy = Lot(timestamp, btc, -usd, t)
            total_buy -= usd
            if args.nowash:
                recent_sells = []
            while recent_sells and buy:
                recent_sell, recent_sell_buy = recent_sells.pop(0)
                if time.mktime(recent_sell.timestamp) < time.mktime(timestamp) - 30*24*60*60:
                    continue
                if recent_sell_buy.usd < recent_sell.usd:
                    continue
                recent_sell, recent_sell_remainder = recent_sell.split(buy.btc)
                recent_sell_buy, recent_sell_buy_remainder = recent_sell_buy.split(buy.btc)
                if recent_sell_remainder:
                    recent_sells.insert(0, (recent_sell_remainder, recent_sell_buy_remainder))
                wash_buy, buy = buy.split(recent_sell.btc)
                loss = recent_sell_buy.usd - recent_sell.usd
                print("Wash sale", recent_sell, wash_buy)
                print("Originally bought at", recent_sell_buy, "loss", loss)
                gains += loss
                dissallowed_loss += loss
                wash_buy.dissallowed_loss = loss
                wash_buy.usd += loss
                gains += push_lot(t.account, wash_buy)
                total_cost += wash_buy.usd - loss
            if not recent_sells and buy:
                gains += push_lot(t.account, buy)
                total_cost += buy.usd
        else:
            to_sell = Lot(timestamp, -btc, usd, t)
            sold_lots = []
            gain = 0
            long_term_gain = 0
            long_term_gift = 0
            lost_in_transfer = t.fee_btc
            while to_sell:
                if not lots[t.account]:
                    # The default account can go negative, treat as a short
                    # to be covered when btc is transfered back in.
                    # TODO(robertwb): Delay the gain until the short is covered.
                    assert t.account == 'bitcoind', (t, to_sell)
                    # Treat short as zero cost basis, loss will occur when count is refilled.
                    buy = Lot(t.timestamp, to_sell.btc, 0, t)
                else:
                    buy = lots[t.account].pop()
                print(buy)
                buy, remaining = buy.split(to_sell.btc)
                sold_lots.append(buy)
                if remaining:
                    lots[t.account].unpop(remaining)
                sell, to_sell = to_sell.split(buy.btc)
                if t.type == 'transfer':
                    if lost_in_transfer:
                        lost, buy = buy.split(lost_in_transfer)
                        if lost:
                            lost_in_transfer -= lost.btc
                    if buy:
                        push_lot(t.dest_account, buy)
                        account_btc[t.dest_account] += buy.btc
                else:
                    gain += sell.usd - buy.usd
                    # TODO: split into long, short term.
                    total_sell += sell.usd
                    total_cost_basis += buy.usd
                    if is_long_term(buy, sell):
                        long_term_gain += sell.usd - buy.usd
                        long_term_cost_basis += buy.usd
                    total_cost -= buy.usd - buy.dissallowed_loss
                    if t.type == 'transfer_out':
                        transfered_out.append((t, buy))
                    elif t.type == 'gift' and is_long_term(buy, sell):
                        long_term_gift += sell.usd - buy.usd
                        long_term_gift_cost_basis += buy.usd
                    else:
                        dissallowed_loss -= buy.dissallowed_loss
                        recent_sells.append((sell, buy))
            gains += gain
            long_term_gains += long_term_gain
            long_term_gifts += long_term_gift
            if t.type == 'gift':
                gift_txns.append((t, sold_lots))
        market_price = fmv(t.timestamp)
        total_btc = sum(account_btc.values())
        print(account_btc)
        print("dissallowed_loss", dissallowed_loss)
        print("total_btc", total_btc, "total_cost", total_cost, "market_price", market_price)
        unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
        print("gains", gains, "long_term_gains", long_term_gains, "unrealized_gains", unrealized_gains, "total", gains + unrealized_gains)
        print()
        by_month.record(t.timestamp, income=income, gross_receipts=gross_receipts,
                        total_buy=total_buy, total_sell=total_sell,
                        unrealized_gains=unrealized_gains,
                        gains=gains, long_term_gains=long_term_gains, short_term_gains=gains-long_term_gains,total_cost=total_cost,
                        total_cost_basis=total_cost_basis, long_term_cost_basis=long_term_cost_basis, short_term_cost_basis=total_cost_basis-long_term_cost_basis,
                        long_term_gifts=long_term_gifts, long_term_gift_cost_basis=long_term_gift_cost_basis,
                        total=income+gains+unrealized_gains)
    save_external(external)

    market_price = fmv(time.gmtime(time.time() - 24*60*60))
    unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
    print("total_btc", total_btc, "total_cost", total_cost, "market_price", market_price)
    print("gains", gains, "unrealized_gains", unrealized_gains)
    print()

    print("Income")
    for date, amount in income_txn:
        print("{date:8} {amount:>12.2f}".format(date=date, amount=amount))

    print("\nGross Receipts")
    for date, amount in gross_receipts_txn:
        print("{date:8} {amount:>12.2f}".format(date=date, amount=amount))

    if args.list_purchases:
        print()
        print("Purchase")
        for t in all:
            if t.id in external:
                data = dict(external[t.id])
                if data['type'] == 'purchase':
                    data['usd'] = decimal_or_none(data['usd'])
                    data['btc'] = decimal_or_none(data['btc'])
                    print("{purchase_date:8} {usd:>10.2f}  {btc:>12.8f}  {account:10}   {info} {note}".format(
                        **data))

    if args.list_gifts:
        print()
        print("Gifts")
        for t, gifted_lots in gift_txns:
            data = dict(external[t.id])
            data['purchase_date'] = data['purchase_date'].split()[0]
            data['usd'] = decimal_or_none(data['usd'])
            data['btc'] = decimal_or_none(data['btc'])
            print("{purchase_date:8} {usd:>10.2f}  {btc:>12.8f}  {account:10}   {info} {note}".format(
                **data))
            cost_basis = sum(lot.usd + lot.dissallowed_loss for lot in gifted_lots)
            print("Cost Basis {usd:>10.2f}".format(usd=cost_basis))
            for lot in gifted_lots:
                print('\t', lot)


    for account, account_lots in sorted(lots.items()):
        print()
        print(account, account_btc[account])
        if account_lots:
            cost_basis = sum(lot.usd for lot in account_lots._data)
            print("cost basis:", round(cost_basis, 2), "fmv:", round(market_price * account_btc[account], 2))
        for lot in account_lots:
            print(lot)

    print()
    for account, account_lots in sorted(lots.items()):
        cost_basis = sum(lot.usd for lot in account_lots._data)
        print(account, account_btc[account], "cost basis:", round(cost_basis, 2), "fmv:", round(market_price * account_btc[account], 2))

    if transfered_out:
        print()
        print()
        print("Transfered out (not yet taxed):")
        last_t = None
        for t, lot in transfered_out:
            if last_t is None or t != last_t:
                print()
                print(time.strftime("%Y-%m-%d %H:%M:%S", t.timestamp), t.btc)
                if t.info:
                    print("   ", t.info)
                if external[t.id]['note']:
                    print("   ", external[t.id]['note'])
                last_t = t

            print("   ", lot)
    print()
    print()

    market_price = fmv(time.gmtime(time.time() - 24*60*60))
    unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
    print("total_btc", total_btc, "total_cost", total_cost, "market_price", market_price)
    print("gains", gains, "unrealized_gains", unrealized_gains)
    print()

#    format = "{date:8} {income:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
#    print format.replace('.2f', '').format(date='date', income='income', gains='realized gains', long_term_gains='long term', unrealized_gains='unrealized', total='total  ')
    names = dict(date='date', income='income', gross_receipts='gross\nreceipts',
        gains='realized\ngains', long_term_gains='long term\ngains',
        long_term_gifts='gift exempt\ngains', long_term_gift_cost_basis='gift exempt\ncost basis',
        unrealized_gains='unrealized\ngains', total='total  ', total_sell='sell',
        total_cost_basis='cost basis', long_term_cost_basis='long term\ncost basis', total_buy='buy',
        short_term_cost_basis='short term\ncost basis', short_term_gains='short term\ngains')
    if args.cost_basis:
        format = "{date:8} {short_term_cost_basis:>12.2f} {short_term_gains:>12.2f} {long_term_cost_basis:>12.2f} {long_term_gains:>12.2f} {long_term_gift_cost_basis:>12.2f} {long_term_gifts:>12.2f} {gains:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
#         print format.replace('.2f', '').format(
#             date='', short_term_cost_basis='', long_term_cost_basis='cost bais',
#             short_term_gains='gains', long_term_gains='', long_term_gifts='', gains='', unrealized_gains='', total='')
    elif args.buy_in_sell_month:
        format = "{date:8} {income:>12.2f} {gross_receipts:>12.2f} {total_cost_basis:>12.2f} {total_sell:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {long_term_gifts:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
    else:
        format = "{date:8} {income:>12.2f} {gross_receipts:>12.2f} {total_buy:>12.2f} {total_sell:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {long_term_gifts:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
    print(format.replace('.2f', '').format(**{name: ''.join(label.split('\n')[:-1]) for name, label in names.items()}))
    print(format.replace('.2f', '').format(**{name: label.split('\n')[-1] for name, label in names.items()}))
    by_month.dump(format)
    print()
    print("Annual")
    by_month.consolidate('%Y').dump(format)
    print()
    by_month.consolidate('All time').dump(format)

    need_appraisal = []
    for year, delta in by_month.consolidate('%Y').deltas().items():
        if delta['long_term_gifts'] >= 5000:
            need_appraisal.append(year)
    if need_appraisal:
        if len(need_appraisal) <= 2:
            need_appraisal_string = ' and '.join(need_appraisal)
        else:
            need_appraisal_string.formatted = ', '.join(need_appraisal[:-1]) + ", and " + need_appraisal[-1]
        print()
        print("A qualified appraisal is needed for charitable deductions in %s." % need_appraisal_string)
        if not args.list_gifts:
            print("Run with --list_gifts for lot details.")
        print()



if __name__ == '__main__':
    parsed_args = parser.parse_args()
    main(parsed_args)
