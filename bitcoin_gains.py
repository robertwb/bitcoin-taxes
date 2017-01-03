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

import argparse
from collections import defaultdict
import csv
import decimal
import heapq
import json
import re
import os
import pprint
import sys
import time
import urllib2
import urlparse

try:
    import readline
except ImportError:
    pass

parser = argparse.ArgumentParser(description='Compute capital gains/losses.')

parser.add_argument('histories', metavar='FILE', nargs='+',
                   help='a csv or json file')

parser.add_argument('--fmv_url', dest='fmv_urls',
                    action='append',
                    default=['https://api.bitcoinaverage.com/history/USD/per_day_all_time_history.csv',
                             'https://blockchain.info/charts/market-price?timespan=all&daysAverageString=1&format=csv'],
                    help='fair market value prices urls')

parser.add_argument('--data', dest='data', default='data.json',
                   help='external transaction info')

parser.add_argument('--transfer_window_hours', default=24)

parser.add_argument('--method', default='fifo', help='used to select which lot to sell; one of fifo, lifo, lowest, highest')

parser.add_argument("-y", "--non_interactive", help="don't prompt the user to confirm external transfer details",
                    action="store_true")

parser.add_argument("--consolidate_bitcoind", help="treat bitcoind accounts as one", action="store_true")

parser.add_argument("--external_transactions_file", default="external_transactions.json")

parser.add_argument("--flat_transactions_file", default="all_transactions.csv")

parser.add_argument("--nowash", default=False, action="store_true")

parser.add_argument("--buy_in_sell_month", default=False, action="store_true")

parser.add_argument("--end_date", metavar="YYYY-MM-DD")

class TransactionParser:
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
        return self.__class__.__name__.replace('Parser', '')
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
        return re.sub(r'\s+', '', open(filename).read(100)).startswith('[{"account":')
    def parse_file(self, filename):
        for item in json.load(open(filename)):
            timestamp = time.localtime(item['time'])
            item['amount'] = decimal.Decimal(item['amount']).quantize(decimal.Decimal('1e-8'))
            item['fee'] = decimal.Decimal(item.get('fee', 0)).quantize(decimal.Decimal('1e-8'))
            info = ' '.join([item.get('to', ''), item.get('comment', ''), item.get('address', '')])
            if not parsed_args.consolidate_bitcoind:
                account = ('bitcoind-%s' % item['account']).strip('-')
            else:
                account = 'bitcoind'
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

class BitcoinInfoParser(TransactionParser):
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
            for ix, input in enumerate(txn['inputs']):
                if input['prev_out']['addr'] == address:
                    # TODO: fee
                    yield Transaction(timestamp, 'withdraw', -decimal.Decimal(input['prev_out']['value']) / satoshi_to_btc, 0, id="%s-%s:%s" % (address, txn['hash'], ix), account=address)
            for ix, output in enumerate(txn['out']):
                if output['addr'] == address:
                    yield Transaction(timestamp, 'deposit', decimal.Decimal(output['value']) / satoshi_to_btc, 0, id="%s-%s:%s" % (address, txn['hash'], ix), account=address)
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
                continue
            else:
                try:
                    transaction = self.parse_row(row)
                    if transaction is not None:
                        yield transaction
                except Exception:
                    print ix, row
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
            raise ValueError, type

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
    expected_header = 'transaction_hash,label,confirmations,value,timestamp'  # 2.5.4

    def parse_row(self, row):
#        transaction_hash,label,confirmations,value,fee,balance,timestamp = row
        transaction_hash,label,confirmations,value,timestamp = row
        # TODO: Why isn't this exported anymore?
        fee = tx_fee(transaction_hash)
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M')
        timestamp = time.localtime(time.mktime(timestamp) + 7*60*60)
        if not label:
            label = 'unknown'
        elif label[0] == '<' and label[-1] != '>':
            label = label[1:]
        common = dict(usd=None, info=label, id=transaction_hash)
        if value[0] == '+':
            return Transaction(timestamp, 'deposit', value[1:], **common)
        else:
            assert value[0] == '-'
            true_value = decimal.Decimal(value) + decimal.Decimal(fee)
            if true_value == 0:
                return Transaction(timestamp, 'fee', fee, **common)
            else:
                return Transaction(timestamp, 'withdraw', true_value, fee_btc=fee, **common)

class CoinbaseParser(CsvParser):
    expected_header = r'(User,.*,[0-9a-f]+)|(^Transactions$)'
    started = False

    def reset(self):
        self.started = False

    def parse_row(self, row):
        if not self.started:
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
        if '$' in note:
            # It's a buy/sell
            if total:
                assert total_currency == 'USD'
                usd = total
            else:
                prices = re.findall(r'\$\d+\.\d+', note)
                if len(prices) != 1:
                    raise ValueError, "Ambiguous or missing price: %s" % note
                usd = prices[0][1:]
            type = 'trade'
            if 'Paid for' in note or 'Bought' in note:
                usd = '-' + usd
        else:
            usd = 0
            type = 'deposit' if float(btc) > 0 else 'withdraw'
        info = " ".join([note, to])
        if True:
            account = self.filename
        else:
            account = None
        return Transaction(timestamp, type, btc, usd, info=info, account=account)


class KrakenParser(CsvParser):

    def start(self):
        self._trades = defaultdict(dict)

    def can_parse(self, filename):
        first_line = open(filename).readline().strip()
        if first_line.endswith('"ledgers"'):
            raise ValueError("Use ledger, not trade, export for Kraken.")
        elif first_line == '"txid","refid","time","type","aclass","asset","amount","fee","balance"':
            return True

    def parse_row(self, row):
        txid, refid, ktimestamp, ktype, _, asset, amount, fee, _ = row
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
            print Transaction(timestamp, ktype, amount, 0)
            return Transaction(timestamp, ktype, amount, 0)
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
            raise ValueError, "mtgox must contain BTC or USD"
        self.seen_file_count[self.is_btc] += 1
        for t in CsvParser.parse_file(self, filename):
            yield t

    def parse_row(self, row):
        ix, timestamp, type, info, value, balance = row
        ix = int(ix)
        if ix in self.seen_transactions[self.is_btc]:
            raise ValueError, "Duplicate tranaction: %s" % ix
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
            raise ValueError, type

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
            print len(transactions)
            for t in transactions:
                print t, t.line
            print merged.__dict__
            raise
        return merged

    def check_complete(self):
        if self.seen_file_count[0] != self.seen_file_count[1]:
            raise ValueError, "Missmatched number of BTC and USD files (%s vs %s)." % tuple(seen_file_count)
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
                raise ValueError, "Missing transactions in mtgox %s history (%s to %s)." % (usd_or_btc[is_btc], gap_start, gap_end-1)

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


zero = decimal.Decimal('0', 8)
tenth = decimal.Decimal('0.1')
satoshi_to_btc = decimal.Decimal('1e8')
def roundd(x, digits):
    return x.quantize(tenth**digits)

def decimal_or_none(o):
    return None if o is None else decimal.Decimal(o)

def strip_or_none(o):
    return o.strip() if o else o

class Transaction():
    def __init__(self, timestamp, type, btc, usd, price=None, fee_usd=0, fee_btc=0, info=None, id=None, account=None, parser=None):
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

    def __cmp__(left, right):
        return cmp(left.timestamp, right.timestamp) or cmp(left.id, right.id)

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
        return "%s(%s, %s, %s, %s%s%s)" % (self.type, time.strftime('%Y-%m-%d %H:%M:%S', self.timestamp), self.usd, self.btc, self.account, fee_str, dest_str)

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

    def __cmp__(left, right):
        if parsed_args.method == 'fifo':
            return cmp(left.timestamp, right.timestamp) or cmp(left.transaction, right.transaction)
        elif parsed_args.method == 'lifo':
            return cmp(right.timestamp, left.timestamp) or cmp(left.transaction, right.transaction)

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

already_forced_download = set()
def open_cached(url, force_download=False):
    global already_forced_download
    if '://' not in url:
        # It's a (possibly relative) file path.
        return open(url)
    parts = urlparse.urlparse(url)
    basename = 'cached-' + parts.hostname + '-' + parts.path.split('/')[-1]
    if not os.path.exists(basename) or (force_download and url not in already_forced_download):
        already_forced_download.add(url)
        handle = urllib2.urlopen(url)
        try:
            open(basename, 'wb').write(handle.read())
        except:
            return urllib2.urlopen(url)
    return open(basename)

prices = {}
def fmv(timestamp):
    date = time.strftime('%Y-%m-%d', timestamp)
    if date not in prices:
        fetch_prices(False)
    if date not in prices:
        fetch_prices(True)
    if date not in prices:
        prev = [d for d in prices if d < date]
        if not prev:
            raise ValueError, "No price for %s" % date
        else:
            date = max(prev)
    return prices[date]

def fetch_prices(force_download=False):
    print "Fetching fair market values..."
    for url in reversed(parsed_args.fmv_urls):
        if not url:
            # Empty parameter ignores all previous.
            break
        print url
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
                    raise ValueError, "Unknown format: %s" % line
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
    print "Done"

tx_fees = {}
def tx_fee(tx_hash):
    global tx_fees
    tx_fee_file = 'tx_fees.json'
    if not tx_fees and os.path.exists(tx_fee_file):
        tx_fees = json.load(open(tx_fee_file))
    if tx_hash in tx_fees:
        return decimal.Decimal(tx_fees[tx_hash])
    else:
        print "Downloading fee for tx", tx_hash
        fee = decimal.Decimal(urllib2.urlopen("https://blockchain.info/q/txfee/" + tx_hash).read().strip()) * decimal.Decimal('1e-8')
        tx_fees[tx_hash] = str(fee)
        json.dump(tx_fees, open(tx_fee_file, 'w'))
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
        last = {}
        for date in sorted(self.data):
            data = self.data[date]
            diff = dict((key, value-last.get(key, 0)) for key, value in data.items())
            print format.format(date=date, **diff)
            last = data
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
        r = raw_input(prompt)
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


def load_external():
    if os.path.exists(parsed_args.external_transactions_file):
        return json.load(open(parsed_args.external_transactions_file))
    else:
        return {}

def save_external(external):
    if not parsed_args.non_interactive:
        json.dump(external, open(parsed_args.external_transactions_file, 'w'), indent=4, sort_keys=True)


def main(args):

    if args.end_date:
        max_timestamp = time.strptime(args.end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
    else:
        max_timestamp = float('inf'),

    parsers = [BitstampParser(), MtGoxParser(), BitcoindParser(), CoinbaseParser(), ElectrumParser(), DbDumpParser(), BitcoinInfoParser(), TransactionParser(), KrakenParser()]
    all = []
    for file in args.histories:
        for parser in parsers:
            if parser.can_parse(file):
                print file, parser
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
            raise RuntimeError, "No parser for " + file
    for parser in parsers:
        parser.check_complete()

    by_date = defaultdict(list)
    for t in all:
        by_date[t.parser, t.id].append(t)
    for key, value in by_date.iteritems():
        by_date[key] = key[0].merge_some(value)
    all = [t for merged in by_date.values() for t in merged]
    all.sort()

    if parsed_args.flat_transactions_file:
        handle = open(parsed_args.flat_transactions_file, 'w')
        handle.write(Transaction.csv_header())
        handle.write('\n')
        for t in all:
            handle.write(t.csv())
            handle.write('\n')
        handle.close()


    deposits = defaultdict(list)
    for t in all:
        if t.type == 'deposit' and t.btc:
            deposits[t.btc].append(t)
#    pprint.pprint(deposits.items())

    for t in list(all):
        if t.type == 'withdraw' and t.btc:
            matches = deposits.get(-t.btc, ())
            for candidate in matches:
                if abs(time.mktime(candidate.timestamp) - time.mktime(t.timestamp)) < args.transfer_window_hours * 3600:
                    matches.remove(candidate)
                    all.remove(t)
                    all.remove(candidate)
                    transfer = Transaction(t.timestamp, 'transfer', t.btc, 0, fee_btc=t.fee_btc, fee_usd=t.fee_usd)
                    transfer.account = t.account
                    transfer.dest_account = candidate.account
                    all.append(transfer)
                    # todo: fee?
                    print 'match', t, candidate
                    break
            else:
                if matches:
                    print "no match", t, matches

    pprint.pprint(sorted([(key, value) for key, value in deposits.items() if value],
                         key=lambda kv: kv[1][0].timestamp))
    for t in all:
        if t.type not in ('trade', 'transfer'):
            print t

    total_cost = 0
    account_btc = defaultdict(int)
    income = 0
    income_txn = []
    gains = 0
    long_term_gains = 0
    long_term_gifts = 0
    total_buy = 0
    total_sell = 0
    total_cost_basis = 0
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
    lots = defaultdict(Heap)
    all.sort()
    pprint.pprint(all[25:35])
    by_month = RunningReport("%Y-%m")
    transfered_out = []
    print
    for ix, t in enumerate(all):
        if exit:
            break
        print ix, t
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
                elif data['type'] == 'gift':
                    t.type = 'gift'
                elif data['type'] in ('buy', 'sale', 'purchase'):
                    t.type = 'trade'
            else:
                price = t.price or fmv(t.timestamp)
                approx_usd = roundd(-price * btc, 2)
                print
                print "On %s you %s %s btc (~$%s at %s/btc)." % (
                    time.strftime("%a, %d %b %Y %H:%M:%S +0000", t.timestamp),
                    ['sent', 'recieved'][t.btc > 0],
                    abs(t.btc),
                    abs(approx_usd),
                    price)
                print t.info
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
                    note = raw_input('Note: ')
                    external[t.id] = { 'usd': str(usd), 'btc': str(btc), 'price': str(price),
                                       'type': type, 'note': note, 'info': t.info, 'account': t.account,
                                       'timestamp': time.strftime('%Y-%m-%d %H:%M:%S', t.timestamp),
                                       'purchase_date': time.strftime('%Y-%m-%d %H:%M:%S', timestamp) }


        print t
        if btc < 0:
            btc -= t.fee_btc
        account_btc[t.account] += btc
        print "btc", btc, "usd", usd
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
                print "Wash sale", recent_sell, wash_buy
                print "Originally bought at", recent_sell_buy, "loss", loss
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
            gain = 0
            long_term_gain = 0
            long_term_gift = 0
            lost_in_transfer = t.fee_btc
            while to_sell:
                if not lots[t.account]:
                    # The default account can go negative, treat as a short
                    # to be covered when btc is transfered back in.
                    # TODO(robertwb): Delay the gain until the short is covered.
                    assert t.account == 'bitcoind', t
                    # Treat short as zero cost basis, loss will occur when count is refilled.
                    buy = Lot(t.timestamp, to_sell.btc, 0, t)
                else:
                    buy = lots[t.account].pop()
                print buy
                buy, remaining = buy.split(to_sell.btc)
                if remaining:
                    lots[t.account].push(remaining)
                sell, to_sell = to_sell.split(buy.btc)
                if t.type == 'transfer':
                    if lost_in_transfer:
                        lost, buy = buy.split(lost_in_transfer)
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
                    total_cost -= buy.usd - buy.dissallowed_loss
                    if t.type == 'transfer_out':
                        transfered_out.append((t, buy))
                    elif t.type == 'gift' and is_long_term(buy, sell):
                        long_term_gift += sell.usd - buy.usd
                    else:
                        dissallowed_loss -= buy.dissallowed_loss
                        recent_sells.append((sell, buy))
            gains += gain
            long_term_gains += long_term_gain
            long_term_gifts += long_term_gift
        market_price = fmv(t.timestamp)
        total_btc = sum(account_btc.values())
        print account_btc
        print "dissallowed_loss", dissallowed_loss
        print "total_btc", total_btc, "total_cost", total_cost, "market_price", market_price
        unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
        print "gains", gains, "long_term_gains", long_term_gains, "unrealized_gains", unrealized_gains, "total", gains + unrealized_gains
        print
        by_month.record(t.timestamp, income=income, gains=gains, long_term_gains=long_term_gains, unrealized_gains=unrealized_gains, total_cost=total_cost, total=income+gains+unrealized_gains,
                        total_buy=total_buy, total_sell=total_sell, total_cost_basis=total_cost_basis, long_term_gifts=long_term_gifts)
    save_external(external)

    market_price = fmv(time.gmtime(time.time() - 24*60*60))
    unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
    print "total_btc", total_btc, "total_cost", total_cost, "market_price", market_price
    print "gains", gains, "unrealized_gains", unrealized_gains
    print

    print "Income"
    for date, amount in income_txn:
        print "{date:8} {amount:>12.2f}".format(date=date, amount=amount)


    for account, account_lots in sorted(lots.items()):
        print
        print account, account_btc[account]
        if account_lots.data:
            cost_basis = sum(lot.usd for lot in account_lots.data)
            print "cost basis:", round(cost_basis, 2), "fmv:", round(market_price * account_btc[account], 2)
        account_lots = Heap(account_lots.data)
        while account_lots:
            print account_lots.pop()

    print
    for account, account_lots in sorted(lots.items()):
        cost_basis = sum(lot.usd for lot in account_lots.data)
        print account, account_btc[account], "cost basis:", round(cost_basis, 2), "fmv:", round(market_price * account_btc[account], 2)

    if transfered_out:
        print
        print
        print "Transfered out (not yet taxed):"
        last_t = None
        for t, lot in transfered_out:
            if last_t is None or t != last_t:
                print
                print time.strftime("%Y-%m-%d %H:%M:%S", t.timestamp), t.btc
                if t.info:
                    print "   ", t.info
                if external[t.id]['note']:
                    print "   ", external[t.id]['note']
                last_t = t

            print "   ", lot
    print
    print

    market_price = fmv(time.gmtime(time.time() - 24*60*60))
    unrealized_gains = market_price * total_btc - total_cost - dissallowed_loss
    print "total_btc", total_btc, "total_cost", total_cost, "market_price", market_price
    print "gains", gains, "unrealized_gains", unrealized_gains
    print

#    format = "{date:8} {income:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
#    print format.replace('.2f', '').format(date='date', income='income', gains='realized gains', long_term_gains='long term', unrealized_gains='unrealized', total='total  ')
    if args.buy_in_sell_month:
        format = "{date:8} {income:>12.2f} {total_cost_basis:>12.2f} {total_sell:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {long_term_gifts:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
    else:
        format = "{date:8} {income:>12.2f} {total_buy:>12.2f} {total_sell:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {long_term_gifts:>12.2f} {unrealized_gains:>12.2f} {total:>12.2f}"
    print format.replace('.2f', '').format(date='date', income='income', gains='realized gains', long_term_gains='long term', long_term_gifts='gift exempt', unrealized_gains='unrealized', total='total  ', total_sell='sell', total_cost_basis='buy', total_buy='buy')
    by_month.dump(format)
    print
    print "Annual"
    by_month.consolidate('%Y').dump(format)
    print
    by_month.consolidate('All time').dump(format)




if __name__ == '__main__':
    parsed_args = parser.parse_args()
    main(parsed_args)
