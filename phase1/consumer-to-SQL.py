from kafka import KafkaConsumer, TopicPartition
from json import loads
import sqlalchemy as db 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


engine = db.create_engine('sqlite:///consumerdb.db') 
conn = engine.connect()

# Create a base for declarative models
# This creates a base class. Any class that inherits from Base will be considered a SQLAlchemy model.
Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transaction'
    id = db.Column(db.Integer, primary_key = True)
    custid = db.Column(db.Integer)
    type = db.Column(db.String(250), nullable=False)
    date = db.Column(db.Integer)
    amt = db.Column(db.Integer)

# Create the table in the database
Base.metadata.create_all(engine)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.
        self.Session = sessionmaker(bind=engine) # Create a Session factory


        #Go back to the readme.

    def handleMessages(self):
        session = self.Session() # Create a new session for each batch of messages (or per message)

        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            
            new_transaction = Transaction(
                custid = message['custid'],
                type = message['type'],
                date = message['date'],
                amt = message['amt']
            )
            session.add(new_transaction)
            session.commit()

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)



    



if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()