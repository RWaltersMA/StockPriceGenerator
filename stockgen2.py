#this doesnt work well, deadlocks and different areas of time are getting newprices based off of old..
import random
import csv
import argparse
import pymongo
#from datetime import datetime, timedelta as dt
from datetime import date, timedelta
from datetime import datetime as dt
import threading
import sys
lock = threading.Lock()

MONGO_URI = 'mongodb://localhost:27020/Stock'

volatility=.005
symbols=[]
last_ticker_price=[]

#this needs to be fixed as it always ^^
def getprice(old_price):
	#rnd=random.uniform(0, 1)
	change_percent = 2 * volatility *  random.random() #rnd #2*
	if (change_percent > volatility):
		change_percent -= (2 * volatility)
	change_amount = old_price * change_percent
	new_price = old_price + change_amount
	if (round(new_price,2).is_integer()):
		return (round(new_price,2)+random.uniform(-.05, .05))
	else:
		return round(new_price,2)

def processfile(fn):
#file that comes from nasdaq has a header and a footer line that we need to ignore
	try:
		with open(fn, "rb") as f:
			row_count = sum(1 for line in f)
			row_count+=1
			f.seek(0) # we counted the lines need to rewind
			f.next() # skip the header row
			reader = csv.reader(f, delimiter="|")
			for i, line in enumerate(reader,0):
				#print 'line[{}] = {}'.format(i, line)
				if (i<=row_count):
					symbols.append(line[0])
					#generate random stock price
					last_ticker_price.append(random.randint(10,75))
	except:
		print('Error occured reading file')
		f.close()

def main():
	#capture parameters from the command line
	parser = argparse.ArgumentParser()
	parser.add_argument("-f", help="file to import")
	parser.add_argument("-d", help="days of stock data to create")
	parser.add_argument("-t", help="number of worker threads")
	
	args = parser.parse_args()
	#fill the symbols array with the ticker symbols we will fill our data with
	processfile(args.f)
	c = pymongo.MongoClient(MONGO_URI)
	db = c.get_database(name='Stock')
	db['StockDocPerSecond'].drop()
	db['StockDocPerMinute'].drop()
	days=int(args.d)
	numofthreads=int(args.t)
	numbertoprocess=int(days/numofthreads)
	threads=[]
	for i in range(0,numofthreads):
		t=threading.Thread(target=worker, args=(i,numbertoprocess,numofthreads,))
		threads.append(t)
		t.start()
	for t2 in threads:
		t2.join()

def worker(g,process,numot):
	global last_ticker_price
	global symbols
	try:
		c = pymongo.MongoClient(MONGO_URI)
		db = c.get_database(name='Stock')
		startday=g*process
		if g==(numot-1):
			endday=((g+1)*process)
		else:
			endday=((g+1)*process)-1

		print 'Worker # ' + str(g) + ', assigned days ' + str(startday) + ' to ' + str(endday)
		for daynumber in range(startday,endday):
			print 'Worker # ' + str(g) + ', processing day ' + str(daynumber) + '\n'
			'''
			if (daynumber % 30==0):
				r=db.command("collStats","StockDocPerMinute")
				#print(r)
				storageSize=int(r['storageSize']) / (1024*1024)
				dataSize=int(r['size']) / (1024*1024)
				docs=int(r['count'])
				indexSize=int(r['totalIndexSize']) / (1024*1024)
				print("Day=" + str(daynumber) + " PER MIN storageSize=" + str(storageSize) + " dataSize=" + str(dataSize) + " docs=" + str(docs) + " indexSize=" + str(indexSize)) 
				r2=db.command("collStats","StockDocPerSecond")
				#print(r)
				storageSize=int(r2['storageSize']) / (1024*1024)
				dataSize=int(r2['size']) / (1024*1024)
				docs=int(r2['count'])
				indexSize=int(r2['totalIndexSize']) / (1024*1024)
				print("Day=" + str(daynumber) + " PER SECOND storageSize=" + str(storageSize) + " dataSize=" + str(dataSize) + " docs=" + str(docs) + " indexSize=" + str(indexSize)) 
			'''
			market_hour=0
			market_minute=0
			market_second=0
			d=date.today() - timedelta(daynumber)
			while market_hour < 24:
				print 'Worker # ' + str(g) + ', processing day ' + str(daynumber) + ' hour + ' + str(market_hour) + '\n'

				#for every stock create a minute work of data
				for stock in range(0,len(symbols)-1):
					doc_per_minute = {
					'symbol': symbols[stock],
					'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute)
					}
					p={} # this will cache a minute worth of data
					#for every second
					for second in range(0,59):
						#add an extra delay so numbers don't change too often
						with lock:
							if (random.uniform(0, 1) > .5):
								newprice=getprice(last_ticker_price[stock])
							else:
								newprice=last_ticker_price[stock]
							last_ticker_price[stock]=newprice
						p.update( { str(second) : newprice } ) # { 'p' : newprice, 'v' : stockvol }})
						doc_per_event = {
							'symbol': symbols[stock],
							'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute, second=second),
							'p': newprice
						}
						#insert the document per event
						db['StockDocPerSecond'].insert_one(doc_per_event)
						#last_ticker_price[stock]=newprice
					#insert document for every one minute
					doc_per_minute.update({'p' : p})
					db['StockDocPerMinute'].insert_one(doc_per_minute)
				market_minute=market_minute+1
				if (market_minute>59):
					market_minute=0
					market_hour=market_hour+1
	except:
		print('Unexpected error:', sys.exc_info()[0])
		raise

main()
# {
# attributes:
# symbol: 
# price: 
# }
# event granulatory - writes document per event, document per minute, per hour
