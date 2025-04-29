# Kafka3-Data

build simple consumer (python)

Fork this repo, then clone from your new fork. 

to get k
```bash
brew install kafka
brew install kafka-python
```

__Nota Bene 2022:__ _If you're running on a M1 Mac, take a look at the bottom of the page._

## Running Kafka

There are a couple shell scripts for running `zookeeper` and `kafka`.
You need to run `zookeeper` first in a terminal by itself.
Then run `kafka` in a different terminal.

Then run the `Producers` and `consumers` each in their own terminals. (as needed)
Steps needed for phase 0:
in terminal navigate to phase0
brew services start kafka

python3 producers.py
python3 consumers.py

## ZipBank Project

We have decided to use Kafka as our main event handling infrastructure for our new bank, ZipBank.

this lab requires you to have a running kafka/zookeeper pair on your machine.

Kafka will take in transactions from various applications, and your job is to create the consumers needed to save all those transactions into a database.

To help, we've provided a test producer that creates random messages and send them into Kafka.

(to do this lab, you will have needed to step through both of these to get kafka and zookeepder running.
    - [Kafka on Mac1](https://yoda.zipcode.rocks/2020/04/20/kafka-on-mac/)
    - [Kafka on Mac2](https://yoda.zipcode.rocks/2020/04/20/kafka-on-mac-2/)
)

### Phase 1

build a kafka consumer in python to use SQLalchemy to store incoming transactions into a SQL Database. 
you may use any SQL DB you're comfortable with. (Postgres, MySQL, RDS ... ?)

We have supplied a simple generation producer that creates random banking transactions:
`./Kafka3-Data/phase1/producer-random-xactions.py`
It produces 20 transactions each time it is run, sending them to the `bank-customer-events` topic.
(Down below, there is a shell line that can be used to create that topic within your running kafka.)
transaction Producer
    a simple user by user producer that generates random deposits and withdrawals on random accounts.

a transaction looks like this in pseudocode:

``` json
{ custid: int, type: W/D, date: now, amt: int }
```

so a couple samples in python Dicts.

``` json
{ custid: 55, type: "Dep", date: 1587398219, amt: 10000 }
{ custid: 55, type: "Wth", date: 1587398301, amt: 2500 }
```

which means:
customer who's id is 55, Deposit, at time 1587398219, a total of $100.00
customer who's id is 55, Withdraw, at time 1587398301, a total of $25.00

those dates are Unix Epoch second timestamps. [Unix Time](https://en.wikipedia.org/wiki/Unix_time)

to create a new kafka topic (the one you need for the phase1 scripts to work. You only need to do this once.)

``` bash
kafka-topics --create \
--zookeeper localhost:2181 \
--replication-factor 1 \
--partitions 1 \
--topic bank-customer-events
```

### Your Phase 1 Mission

these scripts work. kinda. the problem is every time we re-start the consumer, we lose
all the customer data. the reason is the coder doesn't know SQL like you do! so all the data gets
put into in-memory data structures, but every time you restart the consumer script, they get emptied.

you need to use SQL alchemy to add to the Consumer in phase1 what's needed to save that transaction information into a "transaction" table in the SQL DB of your choice.

you probably need to create the "database" and the "table" within your Sql Database, and then
connect to it anytime someone creates a XactionConsumer() object. (so that modifying the __init__ method.)

and that SQLAlchemy  might be something like

``` python
class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)
 ```

 Read through the producer in phase1. See where it is generating random transaction sizes, and random on whether it's a deposit or withdrawal. (and random on what customer id is used for the transaction)

## Phase 2

Build two "analytical" consumers. One, build a consumer that everytime it starts, produces an on-going statistical summary of all the transactions seen by the system. Two, build a "limit" watcher. This is a made-up idea, but the idea is watch for accounts that exceed a certain negative number, say -5000, and print an error message when that happens.

Unless you need to, don't bother to store any of the output or state in the SQL DB, just keep it in memory.

### SummaryConsumer

SummaryConsumer should produce a list of outputs, the status of the
mean (avg) deposits and mean withdrawals across all customers. You should also print the standard deviation of the distribution for both deposits and withdrawals.
As each transaction comes in, print a new status of the numerical summaries.

### LimitConsumer

LimitConsumer should keep track of the customer ids that have current balances greater or equal to the limit supplied to the constructor. The intro suggests -5000 for eaxmple, but you should be able set that with a parameter to the class' Constructor

## Phase 3

Add multiple bank branches (locations) for the production of transactions.

Each branch has a branch id, and a different partition in kafka. The consumers for each partition need to handle their branch's customer's transactions.

The branches also create new customers. Every so often, a create-customer event happens, and the consumer hooked up to that stream has to create a new customer in the database before any transactions get posted to that customer's account.

the topic is `bank-customer-new`
the SQLalchemy might look like

``` python
class Customer(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    custid = Column(Integer, primary_key=True)
    createdate = Column(Integer)
    fname = Column(String(250), nullable=False)
    lname = Column(String(250), nullable=False)
 ```

a couple samples in python Dicts.

``` json
{ custid: 55, createdate: 1587398219, fname: 'Lisa' lname: 'Loopner' }
{ custid: 56, createdate: 1587398301, fname: 'Todd' lname: 'Cushman' }
```


## For M1 Macs

when using `brew`, you _might_ need to change the zookeeper and kafka start routines differently.

For `zookeeper`:

```bash
/opt/homebrew/opt/kafka/bin/zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

And for the `kafka` process:

```bash
 /opt/homebrew/opt/kafka/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties
 ```
