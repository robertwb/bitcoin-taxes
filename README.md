Bitcoin Capital Gains Tax Calculator
====================================

This is a python script that attempts to calculate your capital gains/losses
based on your transaction history(ies).

First, a disclaimer: I am not an accountant or otherwise authorized to give tax advice.
This code is released under the GPL, which in particular disavows all responsibility for the accuracy of these results. That being said, I hope it's helpful and I'll be using it myself.

Method
------

For tax purposes, this script treats bitcoin as an asset. Every sale is a taxable event, and the gains computed by taking the difference between the sale price and cost basis.  As bitcoins are bought and sold in fractional amounts, a sale's gain is computed against one or more buys.

The lot selection method is configurable, defaulting to FIFO (first in first out).  Deposits (e.g. from mining) are considered income unless otherwise specified, and withdrawals sales (again, unless otherwise specified).
Among the options for withdrawals, a purchase is taxed as a sale and an expense
offsets income.  Actual transfers out must be taxed manually when sold externally.
The suggested "fair market" value for these transfers is pulled from https://bitcoinaverage.com/charts.htm#USD|averages (as far back as it goes) and https://blockchain.info/charts/market-price for older history.
When running in interactive mode, user input will be saved in a json file (defaulting to `external_transactions.json`) and not asked again.  This file can be edited manually or removed to
re-evaluate the nature of a transaction.

Transfers between known accounts are automatically detected.

Use
---

First, download all the transaction histories from all your exchanges.  Currently Bitstamp, Coinbase, and MtGox are supported, but more formats can be easily added.  Note that for MtGox both the BTC and USD files must be downloaded (with BTC or USD in the filename) as neither has the full history.  Transactions from your local wallets should also be exported via `bitcoind list transactions '*' 1000000 > local-accounts.json`.  Run the script as

    python bitcoin_gains.py transaction_downloads/*.csv local-accounts.json ...

This script will be interactive, it will ask questions about external transfers.
To accept all the defaults (as explained above) one can use the `-y` option.
At any point you may enter "quit" and it will remember the answers you have
provided so far.
