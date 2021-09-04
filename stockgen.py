from os import error
import random
from random import randint
import argparse
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import timedelta, datetime as dt
import threading
import sys
import time

lock = threading.Lock()

volatility = 1  # .001

#arrays used to store ficticous securities
company_symbol=[]
company_name=[]

#the following two functions are used to come up with some fake stock securities
def generate_symbol(a,n,e):
    for x in range(1,len(a)):
        symbol=str(a[:x]+n[:1]+e[:1])
        if symbol not in company_symbol:
            return symbol

def generate_securities(numberofsecurities):
    with open('adjectives.txt', 'r') as f:
        adj = f.read().splitlines()
    with open('nouns.txt', 'r') as f:
        noun = f.read().splitlines()
    with open('endings.txt', 'r') as f:
        endings = f.read().splitlines()
    for i in range(0,numberofsecurities,1):
        a=adj[randint(0,len(adj)-1)].upper()
        n=noun[randint(0,len(noun))].upper()
        e=endings[randint(0,len(endings)-1)].upper()
        company_name.append(a + ' ' + n + ' ' + e)
        company_symbol.append(generate_symbol(a,n,e))

#this function is used to randomly increase/decrease the value of the stock, tweak the random.uniform line for more dramatic changes
def getvalue(old_value):
    change_percent = volatility * \
        random.uniform(0.0, .001)  # 001 - flat .01 more
    change_amount = old_value * change_percent
    if bool(random.getrandbits(1)):
        new_value = old_value + change_amount
    else:
        new_value = old_value - change_amount
    return round(new_value, 2)

def checkmongodbconnection():
    try:
        c=MongoClient(MONGO_URI, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
        c.admin.command('ismaster')
        time.sleep(2)
        c.close()
        return True
    except:
        print('\nCould not connect to MongoDB.\n\n')
        return False

def main():
    global args
    global MONGO_URI

    parser = argparse.ArgumentParser()
    parser.add_argument("-s","--symbols", type=int, default=5, help="number of financial stock symbols")
    parser.add_argument("-c","--connection",type=str, default='mongodb://127.0.0.1', help="MongoDB connection string")
    parser.add_argument("-d","--database",type=str, default='StockData', help="Name of destination database (default Stock)")	
    parser.add_argument("-col","--collection",type=str, default='Stocks', help="Name of destination collection (default StockData)")
    parser.add_argument("-x","--duration",type=int, default=0, help="Number of minutes of data to generate (default 0 - forever)")
    parser.add_argument("-drop","--drop",help="Clears the destination collection of data before starting",action="store_true")
    parser.add_argument("-ts","--timeseries",help="Creates the collection as a timeseries collection",action="store_true")
    parser.add_argument("-as","--AsString",help="Write the tx time as a string",action="store_true")

    args = parser.parse_args()

    if args.symbols:
        if args.symbols < 1:
            args.symbols = 1

    MONGO_URI=args.connection

    threads = []

    generate_securities(args.symbols)

    if args.AsString & args.timeseries:
      print('\n** Time series collections require timeseries field to be in datetime field, ignoring --AsString parameter **\n\n')

    for i in range(0, 1): # parallel threads
        t = threading.Thread(target=worker, args=[int(i), int(args.symbols)])
        threads.append(t)
    for x in threads:
        x.start()
    for y in threads:
        y.join()

def worker(workerthread, numofsymbols):
    try:
        #Create an initial value for each security
        last_value=[]
        for i in range(0,numofsymbols):
            last_value.append(round(random.uniform(1, 100), 2))

        #Wait until MongoDB Server is online and ready for data
        while True:
            print('Checking MongoDB Connection')
            if checkmongodbconnection()==False:
                print('Problem connecting to MongoDB, sleeping 10 seconds')
                time.sleep(10)
            else:
                break
        print('Successfully connected to MongoDB')

        c = MongoClient(MONGO_URI, server_api=ServerApi("1", strict=False))
        db = c.get_database(name=args.database)
        txtime = dt.now()
        txtime_end=txtime+timedelta(minutes=args.duration)
        if args.drop:
            print('\nDropping collection ' + args.collection + '\n')
            db.drop_collection(args.collection)
        if args.timeseries:
            collection = db.create_collection(name=args.collection,timeseries= {"timeField": "tx_time", "granularity": "seconds"})
            print('Create collection result=' + collection.full_name)
        print('Data Generation Summary:\n{:<12} {:<12}\n{:<12} {:<12}\n{:<12} {:<12}'.format('# symbols',args.symbols,'Database',args.database,'Collection',args.collection))
        print('\n{:<8}  {:<50}'.format('Symbol','Company Name'))
        for x in range(len(company_name)):
          print('{:<8}  {:<50}'.format(company_symbol[x],company_name[x]))
        print('\n{:<12} {:<12}'.format('Start time',txtime.strftime('%Y-%m-%d %H:%M:%S')))
        if args.duration:
            print('{:<12} {:<12}\n'.format('End time',txtime_end.strftime('%Y-%m-%d %H:%M:%S')))
        else:
            print('No end time - run until user stop (control-Z)\n\n')
        counter=0
        bContinue=True
        while bContinue:
            for i in range(0,numofsymbols):
                #Get the last value of this particular security
                x = getvalue(last_value[i])
                last_value[i] = x
                try:
                    if args.AsString:
                      result=db[args.collection].insert_one( {'company_symbol' : company_symbol[i], 'company_name': company_name[i],'price': x, 'tx_time': txtime.strftime('%Y-%m-%dT%H:%M:%SZ')})
                    else:
                      result=db[args.collection].insert_one( {'company_symbol' : company_symbol[i], 'company_name': company_name[i],'price': x, 'tx_time': txtime})
                    counter=counter+1
                    if counter%100==0:
                      if args.duration>0:
                        print('Generated ' + str(counter) + ' samples ({0:.0%})'.format(counter/(numofsymbols*args.duration*60)))
                      else:
                        print('Generated ' + str(counter))
                    if args.duration>0:
                      if txtime > txtime_end:
                        bContinue=False
                        continue
                except Exception as e:
                    print("error: " + str(e))
            txtime+=timedelta(seconds=1)
        duration=txtime - dt.now()
        print('\nFinished - ' + str(duration).split('.')[0])
    except:
        print('Unexpected error:', sys.exc_info()[0])
        raise

if __name__ == '__main__':
    main()