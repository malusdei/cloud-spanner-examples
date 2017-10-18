#!/usr/bin/env python2.7

import datetime
import pprint
import random
from google.cloud import spanner
from google.cloud.proto.spanner.v1 import type_pb2

"""
This files assumes a schema:
# NOTE THAT COMMENTS '#' MUST BE REMOVED
CREATE TABLE Customers (
 CustomerNumber INT64 NOT NULL,
 LastName STRING(MAX),
 FirstName STRING(MAX),
) PRIMARY KEY (CustomerNumber);
# Note that 'Balance' is a performance optimization in some ways,
# as it could be reconstructed from AccountHistory
CREATE TABLE Accounts (
 CustomerNumber INT64 NOT NULL,
 AccountNumber INT64 NOT NULL,
 CreationTime TIMESTAMP NOT NULL,
 AccountType INT64 NOT NULL,  # (0 - savings, 1 - checking)
 Balance INT64 NOT NULL, # (cents)
 # only necessary for compute_interest_for_all
 LastInterestCalculation TIMESTAMP
) PRIMARY KEY (CustomerNumber, AccountNumber),
  INTERLEAVE IN PARENT Customers;
# enforce that all account numbers are unique
CREATE UNIQUE INDEX UniqueAccountNumbers on Accounts(AccountNumber);
# Bank Transaction history for each account.
# Note: a viable alternative would be to interleave this table in Accounts
# Note that we store ts DESC as primary key because this makes
# iterating latest-first faster.
CREATE TABLE AccountHistory (
  AccountNumber INT64 NOT NULL,
  Ts TIMESTAMP NOT NULL,
  Memo STRING(MAX),
  ChangeAmount INT64 NOT NULL  # cents; positive=credit, negative=debit
)  PRIMARY KEY (AccountNumber, ts DESC);
# A "sharded counter" for tracking balance across all accounts.
# (This is faster than scanning entire accounts table, if Accounts is large)
# Only used for Lab 3, Exercise 2
CREATE TABLE AggregateBalance (
  Shard INT64 NOT NULL,
  Balance INT64 NOT NULL
) PRIMARY KEY (Shard);
"""

# if zero, then don't process/use this table at all.
AGGREGATE_BALANCE_SHARDS = 16

class NegativeBalance(Exception):
    pass

class RowAlreadyUpdated(Exception):
    pass

class NoResults(Exception):
    pass

class TooManyResults(Exception):
    pass

class Unsupported(Exception):
    pass


def generate_int64():
    # Only generate positive numbers (because negative numbers are ugly).
    # Because of the birthday paradox, we don't expect to see duplicates
    # until we have 2^31.5 customers (3 billion), which is fine.
    # More information on choosing a good primary key for your table is here:
    # https://stackoverflow.com/questions/43377489/easiest-primary-key-for-main-table
    return random.randrange(0, (1<<63)-1)


def generate_customer_number():
    return generate_int64()


def generate_account_number():
    return generate_int64()


CUSTOMERS = [generate_customer_number() for i in range(5)]
ACCOUNTS = [generate_account_number() for i in range(5)]


def clear_tables(database):
    with database.batch() as batch:
        batch.delete(
            table='AccountHistory',
            keyset=spanner.KeySet(all_=True))
        batch.delete(
            table='Accounts',
            keyset=spanner.KeySet(all_=True))
        batch.delete(
            table='Customers',
            keyset=spanner.KeySet(all_=True))
        if AGGREGATE_BALANCE_SHARDS > 0:
            batch.delete(
                table='AggregateBalance',
                keyset=spanner.KeySet(all_=True))


def setup_customers(database):
    """Inserts sample data into the given database.
    The database and table must already exist and can be created using
    `create_database`.
    """
    clear_tables(database)
    with database.batch() as batch:
        batch.insert(
            table='Customers',
            columns=('CustomerNumber', 'FirstName', 'LastName',),
            values=[
                (CUSTOMERS[0], u'Marc', u'Richards'),
                (CUSTOMERS[1], u'Catalina', u'Smith'),
                (CUSTOMERS[2], u'Alice', u'Trentor'),
                (CUSTOMERS[3], u'Lea', u'Martin'),
                (CUSTOMERS[4], u'David', u'Lomond')])

        batch.insert(
            table='Accounts',
            columns=('CustomerNumber', 'AccountNumber', 'AccountType',
                     'Balance', 'CreationTime', 'LastInterestCalculation'),
            values=[
                # two accounts for one customer, just to ensure this works
                (CUSTOMERS[0], ACCOUNTS[0], 0, 0,
                 datetime.datetime.utcnow(), None),
                (CUSTOMERS[0], ACCOUNTS[1], 1, 0,
                 datetime.datetime.utcnow(), None),
                (CUSTOMERS[1], ACCOUNTS[2], 0, 0,
                 datetime.datetime.utcnow(), None),
                (CUSTOMERS[2], ACCOUNTS[3], 1, 0,
                 datetime.datetime.utcnow(), None),
                (CUSTOMERS[3], ACCOUNTS[4], 0, 0,
                 datetime.datetime.utcnow(), None),
                ])

        batch.insert(
            table='AccountHistory',
            columns=('AccountNumber', 'Ts', 'ChangeAmount', 'Memo'),
            values = [(ACCOUNTS[i], datetime.datetime.utcnow(), 0,
                       'New Account Initial Deposit') for i in range(5)])
        if AGGREGATE_BALANCE_SHARDS > 0:
            batch.insert(
                table='AggregateBalance',
                columns=('Shard', 'Balance'),
                values=[(i, 0) for i in range(AGGREGATE_BALANCE_SHARDS)])

    print('Inserted data.')


def extract_single_row_to_tuple(results):
    is_ret_set = False
    for row in results:
        if is_ret_set:
            raise TooManyResults
        ret = tuple(row)
        is_ret_set = True
    if not is_ret_set:
        raise NoResults
    return ret


def extract_single_cell(results):
    return extract_single_row_to_tuple(results)[0]


def account_balance(database, account_number):
    params = {'account': account_number}
    param_types = {'account': type_pb2.Type(code=type_pb2.INT64)}
    
    with database.snapshot() as snapshot:
        results=snapshot.execute_sql(
        """SELECT Balance From Accounts@{FORCE_INDEX=UniqueAccountNumbers}
           WHERE AccountNumber=@account""",
            params=params, 
            param_types=param_types
            )

    balance = extract_single_cell(results)
    print("ACCOUNT BALANCE", balance)
    return balance


def customer_balance(database, customer_number):
    """Note: We could implement this method in terms of account_balance,
    but we explicitly want to demonstrate using JOIN"""
    params = {'customer': customer_number}
    param_types = {'customer': type_pb2.Type(code=type_pb2.INT64)}
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            """SELECT SUM(Accounts.Balance) From Accounts INNER JOIN Customers
               ON Accounts.CustomerNumber=Customers.CustomerNumber
               WHERE Customers.CustomerNumber=@customer""",
            params=params, param_types=param_types)
        balance = extract_single_cell(results)
    print("CUSTOMER BALANCE", balance)
    return balance


def last_n_transactions(database, account_number, n):
    params = {
        'account': account_number,
        'num': n,
    }
    param_types = {
        'account': type_pb2.Type(code=type_pb2.INT64),
        'num': type_pb2.Type(code=type_pb2.INT64),
    }
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            """SELECT Ts, ChangeAmount, Memo FROM AccountHistory
               WHERE AccountNumber=@account ORDER BY Ts DESC LIMIT @num""",
            params=params, param_types=param_types)
        ret = list(results)
    print("RESULTS", ret)
    pprint.pprint(ret)
    return ret


def deposit_helper(transaction, customer_number, account_number, cents, memo,
                   new_balance, timestamp):
    transaction.update(
        table='Accounts',
        columns=('CustomerNumber', 'AccountNumber', 'Balance'),
        values=[
            (customer_number, account_number, new_balance),
            ])

    transaction.insert(
        table='AccountHistory',
        columns=('AccountNumber', 'Ts', 'ChangeAmount', 'Memo'),
        values=[
            (account_number, timestamp, cents, memo),
            ])
    if AGGREGATE_BALANCE_SHARDS > 0:
        # Note that one downside of using a sharded counter is that
        # it typically involves an extra machine in your deposit transactions.
        # You are basically trading off write efficiency for read efficiency.
        shard = random.randint(0, AGGREGATE_BALANCE_SHARDS - 1)
        results = transaction.execute_sql(
            "SELECT Balance FROM AggregateBalance WHERE Shard=%d" % shard)
        old_agg_balance = extract_single_cell(results)
        new_agg_balance = old_agg_balance + cents
        transaction.update(
            table='AggregateBalance',
            columns=('Shard', 'Balance'),
            values=[(shard, new_agg_balance)])


def deposit(database, customer_number, account_number, cents, memo=None):
    def deposit_runner(transaction):
        results = transaction.execute_sql(
            """SELECT Balance From Accounts
               WHERE AccountNumber={account_number}
               AND CustomerNumber={customer_number}""".format(
                   account_number=account_number,
                   customer_number=customer_number))
        old_balance = extract_single_cell(results)
        new_balance = old_balance + cents
        if cents < 0 and new_balance < 0:
            raise NegativeBalance
        deposit_helper(transaction, customer_number, account_number, cents,
                       memo, new_balance, datetime.datetime.utcnow())

    database.run_in_transaction(deposit_runner)
    print('Transaction complete.')


def compute_interest_for_account(transaction, customer_number, account_number,
                                 last_interest_calculation):
    # re-check (within the transaction) that the account has not been
    # updated for the current month
    results = transaction.execute_sql(
        """
    SELECT Balance, CURRENT_TIMESTAMP() FROM Accounts
    WHERE CustomerNumber=@customer AND AccountNumber=@account AND
          (LastInterestCalculation IS NULL OR
           LastInterestCalculation=@calculation)""",
        params={'customer': customer_number,
                'account': account_number,
                'calculation': last_interest_calculation},
        param_types={'customer': type_pb2.Type(code=type_pb2.INT64),
                     'account': type_pb2.Type(code=type_pb2.INT64),
                     'calculation': type_pb2.Type(code=type_pb2.TIMESTAMP)})
    try:
        old_balance, current_timestamp = extract_single_row_to_tuple(results)
    except NoResults:
        # An exception means that the row has already been updated.
        # Abort the transaction.
        raise RowAlreadyUpdated

    # Ignoring edge-cases around new accounts and pro-rating first month
    cents = int(0.01 * old_balance)  # monthly interest 1%
    new_balance = old_balance + cents
    deposit_helper(transaction, customer_number, account_number,
                   cents, 'Monthly Interest', new_balance, current_timestamp)

    transaction.update(
        table='Accounts',
        columns=('CustomerNumber', 'AccountNumber', 'LastInterestCalculation'),
        values=[(customer_number, account_number, current_timestamp)])


def compute_interest_for_all(database):
    # In a real production DB, we would use larger batches.
    batch_size = 2
    while True:
        # Find any account that hasn't been updated for the current month
        # (This is done in a read-only transaction, and hence does not
        # take locks on the table)
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                """
            SELECT CustomerNumber,AccountNumber,LastInterestCalculation FROM Accounts
            WHERE LastInterestCalculation IS NULL OR
            (EXTRACT(MONTH FROM LastInterestCalculation) <>
            EXTRACT(MONTH FROM CURRENT_TIMESTAMP()) AND
            EXTRACT(YEAR FROM LastInterestCalculation) <>
            EXTRACT(YEAR FROM CURRENT_TIMESTAMP()))
            LIMIT @batch_size""",
                params={'batch_size': batch_size},
                param_types={'customer': type_pb2.Type(code=type_pb2.INT64)})
        zero_results = True
        for customer_number, account_number, last_calculation in results:
            zero_results = False
            try:
                database.run_in_transaction(compute_interest_for_account,
                                            customer_number, account_number,
                                            last_calculation)
                print("Computed interest for account ", account_number)
            except RowAlreadyUpdated:
                print("Account {account_number} already updated".format(
                        account_number=account_number))
        if zero_results:
            break

def verify_consistent_balances(database):
    if AGGREGATE_BALANCE_SHARDS > 0:
        with database.snapshot() as snapshot:
            balance_slow = extract_single_cell(
                snapshot.execute_sql("SELECT SUM(Balance) FROM Accounts"))
        with database.snapshot() as snapshot:
            balance_fast = extract_single_cell(
                snapshot.execute_sql("SELECT SUM(Balance) FROM AggregateBalance"))
        print("verifying that balances match: ", balance_slow, balance_fast)
        assert balance_fast == balance_slow


def total_bank_balance(database):
    if AGGREGATE_BALANCE_SHARDS <= 0:
        raise Unsupported("There is no fast way to compute aggregate balance")
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql("""SELECT SUM(Balance) From AggregateBalance""")
        balance = extract_single_cell(results)
    print("TOTAL BANK BALANCE", balance)
    return balance


def main():
    # Instantiate a client.
    spanner_client = spanner.Client()

    # Your Cloud Spanner instance ID.
    instance_id = 'my-instance'

    # Get a Cloud Spanner instance by ID.
    instance = spanner_client.instance(instance_id)

    # Your Cloud Spanner database ID.
    database_id = 'my-database'

    # Get a Cloud Spanner database by ID.
    database = instance.database(database_id)

    setup_customers(database)
    print ("Printing Account Balance")
    account_balance(database, ACCOUNTS[1])

    customer_balance(database, CUSTOMERS[0])
    deposit(database, CUSTOMERS[0], ACCOUNTS[0], 150, 'Dollar Fifty Deposit')

    try:
        deposit(database, CUSTOMERS[0], ACCOUNTS[0], -5000,
                'THIS SHOULD FAIL')
    except NegativeBalance:
        print("Properly failed to go to negative balance")

    deposit(database, CUSTOMERS[0], ACCOUNTS[1], 75)
    for i in range(20):
        deposit(database, CUSTOMERS[2], ACCOUNTS[3], i * 100,
                'Deposit %d dollars' % i)
    account_balance(database, ACCOUNTS[1])
    customer_balance(database, CUSTOMERS[0])
    last_n_transactions(database, ACCOUNTS[3], 10)
    compute_interest_for_all(database)
    total_bank_balance(database)
    verify_consistent_balances(database)


if __name__ == "__main__":
    main()
  
