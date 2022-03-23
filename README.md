Bitcoin Capital Gains Tax Calculator
====================================

This is a Python script that attempts to calculate your capital gains/losses
based on your transaction history(ies).

First, a disclaimer: I am not an accountant or otherwise authorized to give tax advice.
This code is released under the GPL, which in particular disavows all responsibility for the accuracy of these results.
That being said, I hope it's helpful and I've been using it for years myself.

Method
------

For tax purposes, this script treats bitcoin as personal property,
consistent with [IRS Notice 2014-21](https://www.irs.gov/newsroom/irs-virtual-currency-guidance).
Every sale is a taxable event, and the gains computed by taking the difference between the sale price and cost basis.
As bitcoins are bought and sold in fractional amounts, a sale's gain is computed against one or more buys.

The lot selection method is configurable, defaulting to FIFO (first in first out).
Deposits (e.g. from mining) are considered income unless otherwise specified,
and withdrawals sales (again, unless otherwise specified).
Among the options for withdrawals, a purchase is taxed as a sale and an expense
offsets income.  Actual transfers out must be taxed manually when sold externally.
The suggested "fair market" value for these transfers is pulled from
https://blockchain.info/charts/market-price.
When running in interactive mode, user input will be saved in a JSON file
(defaulting to `external_transactions.json`) and not asked again.
This file can be edited manually or removed to
re-evaluate the nature of a transaction.

Transfers between known accounts are automatically detected.

A final report is generated, listing the total gains and losses per month/year/all time,
the lots in each account, etc.
Realized long-term gains (or losses) are split out in separate column, short term
gains are the difference between realized gains and long-term gains.
Long-term gift exempt gains are also split out for ease in itemizing charitable
contributions.
(Note that for gifts exceeding $5,000 in value, a qualified appraisal
and signed Form 8283 is also required to claim the deduction.
Some options are [CharitableSolutions](http://charitablesolutionsllc.com/virtual-currency-appraisals/)
or [CryptoAppraisers](https://cryptoappraisers.com/)
or finding another appraiser willing to do the research on Bitcoin.)
This should be sufficient information to file your taxes.
There are a couple of options listed in `--help` to provide even more detailed
reports and options.  In particular, the `--cost_basis` flag is quite useful.


Use
---

First, download/export all the transaction histories from all your exchanges/wallets.
Several exchanges (e.g. Bitstamp, Coinbase, Gdax, Kraken, and MtGox) are supported; more formats can be easily added.
Note that for MtGox both the BTC and USD files must be downloaded (with BTC or USD in the filename) as neither has the full history.
Transactions exported from your local wallets such as Electrum and Bitcoin Core can be imported (e.g. `bitcoin-cli listtransactions '*' 1000000 > local-accounts.json`).
Additionally, any set of public addresses (e.g. from a wallet that does not support export)
can be treated as a single account by simply enumerating them in a text file.

Run the script as

    python bitcoin_gains.py transaction_downloads/*.csv local-accounts.json ...

This script will be interactive, it will ask questions about external transfers.
To accept all the defaults (as explained above) one can use the `-y` option.
At any point you may enter "quit" and it will remember the answers you have
provided so far.
