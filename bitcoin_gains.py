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
import time
import urllib2

parser = argparse.ArgumentParser(description='Compute capital gains/losses.')

parser.add_argument('histories', metavar='FILE', nargs='+',
                   help='a csv or json file')

#parser.add_argument('--fmv', dest='fmv_url', default='https://api.bitcoinaverage.com/history/USD/per_day_all_time_history.csv',
#                   help='fair market value prices url')
#parser.add_argument('--fmv', dest='fmv_url', default='./per_day_all_time_history.csv',
#                   help='fair market value prices url')
# https://blockchain.info/charts/market-price?showDataPoints=false&timespan=all&show_header=true&daysAverageString=1&scale=0&format=csv&address=
parser.add_argument('--fmv_url', dest='fmv_url', default='./blockchaing-market-price.csv',
                   help='fair market value prices url')

parser.add_argument('--data', dest='data', default='data.json',
                   help='external transaction info')

parser.add_argument('--transfer_window_hours', default=24)

parser.add_argument('--method', default='fifo', help='used to select which lot to sell; one of fifo, lifo, lowest, highest')

parser.add_argument("-y", "--non_interactive", help="don't prompt the user to confirm external transfer details",
                    action="store_true")

parser.add_argument("--consolidate_bitcoind", help="treat bitcoind accounts as one", action="store_true")

class TransactionParser:
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

class CsvParser(TransactionParser):
    expected_header = None
    def can_parse(self, filename):
        return re.match(self.expected_header, open(filename).readline().strip())
    def parse_row(self, row):
        raise NotImplementedError
    def parse_file(self, filename):
        first = True
        for row in csv.reader(open(filename)):
            if not row or first:
                first = False
                continue
            else:
                transaction = self.parse_row(row)
                if transaction is not None:
                    yield transaction

class BitstampParser(CsvParser):
    expected_header = 'Type,Datetime,BTC,USD,BTC Price,FEE'

    def parse_row(self, row):
        type, timestamp, btc, usd, price, fee = row
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        if type == '0':
            return Transaction(timestamp, 'deposit', btc, 0, 0)
        elif type == '1':
            return Transaction(timestamp, 'withdraw', btc, 0, 0)
        elif type == '2':
            return Transaction(timestamp, 'trade', btc, usd, price, fee)
        else:
            raise ValueError, type

class CoinbaseParser(CsvParser):
    expected_header = r'User,.*,[0-9a-f]+'

    def parse_row(self, row):
        if ','.join(row).startswith('Timestamp,Balance,BTC Amount'):
            # Coinbase has two header lines.
            return None
        timestamp, _, btc, to, note, total, total_currency = row[:7]
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
            if 'Paid for' in note:
                usd = '-' + usd
        else:
            usd = 0
            type = 'deposit' if float(btc) > 0 else 'withdraw'
        info = " ".join([note, to])
        return Transaction(timestamp, type, btc, usd, info=info)


class MtGoxParser(CsvParser):
    expected_header = 'Index,Date,Type,Info,Value,Balance'

    seen_usd = 0
    seen_btc = 0
    seen_first = [False, False]

    def parse_file(self, filename):
        basename = os.path.basename(filename).upper()
        if 'BTC' in basename:
            self.is_btc = True
            self.seen_btc += 1
        elif 'USD' in basename:
            self.is_btc = False
            self.seen_usd += 1
        else:
            raise ValueError, "mtgox must contain BTC or USD"
        for t in CsvParser.parse_file(self, filename):
            yield t

    def parse_row(self, row):
        ix, timestamp, type, info, value, balance = row
        if ix == '1':
            self.seen_first[self.is_btc] = True
        timestamp = time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        value = decimal.Decimal(value)
        m = re.search(r'tid:\d+', info)
        if m:
            id = m.group(0)
        else:
            id = unique()
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
        if self.seen_usd != self.seen_btc:
            raise ValueError, "Missmatched number of BTC and USD files (%s vs %s)." % (self.seen_btc, self.seen_usd)
        if not all(self.seen_first):
            raise ValueError, "Missing first transaction. (Did you download the > 3 month csv?)"


_unique = 0
def unique():
    global _unique
    _unique += 1
    return "unique:%s" % _unique

tenth = decimal.Decimal('0.1')
def roundd(x, digits):
    return x.quantize(tenth**digits)

def decimal_or_none(o):
    return None if o is None else decimal.Decimal(o)

class Transaction():
    def __init__(self, timestamp, type, btc, usd, price=None, fee_usd=0, fee_btc=0, info=None, id=None, account=None):
        self.timestamp = timestamp
        self.type = type
        self.btc = decimal_or_none(btc)
        self.usd = decimal_or_none(usd)
        self.price = decimal_or_none(price)
        self.fee_usd = decimal_or_none(fee_usd)
        self.fee_btc = decimal_or_none(fee_btc)
        self.info = info
        if self.btc and self.usd and self.price is None:
            self.price = self.usd / self.btc
        if id is None:
            id = unique()
        self.id = id
        self.account = account

    def __cmp__(left, right):
        return cmp(left.timestamp, right.timestamp) or cmp(left.id, right.id)

    def __str__(self):
        return "%s(%s, %s, %s, %s)" % (self.type, time.strftime('%Y-%m-%d %H:%M:%S', self.timestamp), self.usd, self.btc, self.account)

    __repr__ = __str__

class Lot:
    def __init__(self, timestamp, btc, usd, transaction):
        self.timestamp = timestamp
        self.btc = btc
        self.usd = usd
        self.price = usd / btc
        self.transaction = transaction

    def split(self, btc):
        """
        Splits this lot into two, with the first consisting of at most btc bitcoins.
        """
        if btc <= 0:
            return None, self
        elif btc < self.btc:
            usd = roundd(self.price * btc, 2)
            return (Lot(self.timestamp, btc, usd, self.transaction),
                    Lot(self.timestamp, self.btc - btc, self.usd - usd, self.transaction))
        else:
            return self, None

    def __cmp__(left, right):
        if parsed_args.method == 'fifo':
            return cmp(left.timestamp, right.timestamp) or cmp(left.transaction, right.transaction)
        elif parsed_args.method == 'lifo':
            return cmp(right.timestamp, left.timestamp) or cmp(left.transaction, right.transaction)

    def __str__(self):
        return "Lot(%s, %s, %s)" % (time.strftime('%Y-%m-%d', self.timestamp), self.btc, self.price)

    __repr__ = __str__

# Why is this not a class?
class Heap:
    def __init__(self):
        self.data = []
    def push(self, item):
        heapq.heappush(self.data, item)
    def pop(self):
        return heapq.heappop(self.data)
    def __len__(self):
        return len(self.data)

prices = {}
def fmv(timestamp):
    format = None
    if not prices:
        print "Fetching fair market values...",
        for line in (urllib2.urlopen if '://' in parsed_args.fmv_url else open)(parsed_args.fmv_url):
            lint = line.strip()
            if not line:
                continue
            if format is None:
                if line == 'datetime,high,low,average,volume':
                    format = 'bitcoinaverage'
                    continue
                elif re.match(r'\d\d/\d\d/\d\d\d\d \d\d:\d\d:\d\d,\d+\.\d*', line):
                    format = 'blockchain'
                else:
                    raise ValueError, "Unknown format: %s" % line
            cols = line.strip().split(',')
            if format == 'bitcoinaverage':
                date = cols[0].split()[0]
                prices[date] = (decimal.Decimal(cols[1]) + decimal.Decimal(cols[2])) / 2
            else:
                date = '-'.join(reversed(cols[0].split()[0].split('/')))
                price = cols[1]
            prices[date] = decimal.Decimal(price)
        print "Done"
    date = time.strftime('%Y-%m-%d', timestamp)
    return prices.get(date, 100)

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


def main(args):

    parsers = [BitstampParser(), MtGoxParser(), BitcoindParser(), CoinbaseParser()]
    all = []
    for file in args.histories:
        for parser in parsers:
            if parser.can_parse(file):
                print file, parser
                for transaction in parser.parse_file(file):
                    transaction.parser = parser
                    if transaction.account is None:
                        transaction.account = parser.default_account()
                    all.append(transaction)
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
                    transfer = Transaction(t.timestamp, 'transfer', t.btc, 0, fee_usd=t.fee_usd)
                    transfer.account = t.account
                    transfer.dest_account = candidate.account
                    all.append(transfer)
                    # todo: fee?
                    print 'match', t, candidate
                    break
            else:
                if matches:
                    print "no match", t, matches

    pprint.pprint([(key, value) for key, value in deposits.items() if value])
    for t in all:
        if t.type not in ('trade', 'transfer'):
            print t

    total_cost = 0
    account_btc = defaultdict(int)
    income = 0
    gains = 0
    long_term_gains = 0

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

    lots = defaultdict(Heap)
    all.sort()
    pprint.pprint(all[25:35])
    by_month = RunningReport("%Y-%m")
    by_year = RunningReport("%Y")
    print
    for ix, t in enumerate(all):
        print ix, t
        if t.type == 'trade':
            usd, btc = t.usd + t.fee_usd, t.btc
        elif t.type == 'transfer':
            usd, btc = 0, t.btc
        else:
            btc = t.btc
            usd = roundd(-(t.price or fmv(t.timestamp)) * btc, 2)
        if t.type == 'deposit':
            income -= usd
        account_btc[t.account] += btc
        print "btc", btc, "usd", usd
        if btc == 0:
            continue
        elif btc > 0:
            gains += push_lot(t.account, Lot(t.timestamp, btc, -usd, t))
            total_cost -= usd
        else:
            to_sell = Lot(t.timestamp, -btc, usd, t)
            gain = 0
            long_term_gain = 0
            while to_sell:
                if not lots[t.account]:
                    # The default account can go negative, treat as a short
                    # to be covered when btc is transfered back in.
                    # TODO(robertwb): Delay the gain until the short is covered.
                    assert t.account == 'bitcoind'
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
                    push_lot(t.dest_account, buy)
                    account_btc[t.dest_account] += buy.btc
                else:
                    gain += sell.usd - buy.usd
                    if is_long_term(buy, sell):
                        long_term_gain += sell.usd - buy.usd
                    total_cost -= buy.usd
            gains += gain
            long_term_gains += long_term_gain
        market_price = fmv(t.timestamp)
        total_btc = sum(account_btc.values())
        print account_btc
        print "total_btc", total_btc, "total_cost", total_cost, "market_price", market_price
        print "gains", gains, "long_term_gains", long_term_gains, "unrealized_gains", market_price * total_btc - total_cost, "total", gains + market_price * total_btc - total_cost
        print
        unrealized_gains = market_price * total_btc - total_cost
        by_month.record(t.timestamp, income=income, gains=gains, long_term_gains=long_term_gains, unrealized_gains=unrealized_gains, total_cost=total_cost)

    market_value = fmv(time.gmtime(time.time() - 24*60*60))
    print "total_btc", total_btc, "total_cost", total_cost, "market_price", market_price
    print "gains", gains, "unrealized_gains", market_price * total_btc - total_cost
    print


    for account, account_lots in sorted(lots.items()):
        print
        print account, account_btc[account]
        while account_lots:
            print account_lots.pop()

    print
    print
    format = "{date:8} {income:>12.2f} {gains:>12.2f} {long_term_gains:>12.2f} {unrealized_gains:>12.2f}"
    print format.replace('.2f', '').format(date='date', income='income', gains='realized gains', long_term_gains='long term', unrealized_gains='unrealized')
    by_month.dump(format)
    print
    print "Annual"
    by_month.consolidate('%Y').dump(format)



if __name__ == '__main__':
    parsed_args = parser.parse_args()
    if not parsed_args.non_interactive:
        raise NotImplementedError, "interactive"
    main(parsed_args)
