import random
import csv
import argparse
import pymongo
from datetime import date, timedelta
from datetime import datetime as dt
import threading
import sys
import json

lock = threading.Lock()

MONGO_URI = 'mongodb://localhost:27020/Stock'

volatility=.001
symbols=[]
last_ticker_price=[]

def getprice(old_price):
	change_percent = 2 * volatility *  random.random() #rnd #2*
	if (change_percent > volatility):
		change_percent -= (2 * volatility)
	change_amount = old_price * change_percent
	new_price = old_price + change_amount
	if (round(new_price,2).is_integer()):
		if (random.uniform(0, 1) > .5):
			new_price+=.01
		else:
			new_price-=.01
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
					last_ticker_price.append(random.randint(10,25))
	except:
		print('Error occured reading file')
		f.close()

def main():
	#capture parameters from the command line
	parser = argparse.ArgumentParser()
	parser.add_argument("-f", help="file to import")
	parser.add_argument("-d", help="days of stock data to create")
	
	args = parser.parse_args()
	#fill the symbols array with the ticker symbols we will fill our data with
	processfile(args.f)
	c = pymongo.MongoClient(MONGO_URI)
	db = c.get_database(name='Stock')
	db['StockDocPerSecond'].drop()
	db['StockDocPerMinute'].drop()
	db['mystats'].drop()
	days=int(args.d)
	numofthreads=len(symbols)
	threads=[]
	for i in range(0,numofthreads):
		t=threading.Thread(target=worker, args=(i,days))
		threads.append(t)
		t.start()
	for t2 in threads:
		t2.join()

def worker(stock,d):
	global last_ticker_price
	global symbols
	try:
		c = pymongo.MongoClient(MONGO_URI)
		db = c.get_database(name='Stock')

		print 'Worker # ' + str(stock) + ', assigned stock ' + str(symbols[stock])
		for daynumber in range(d,0,-1):
			print 'Worker # ' + str(stock) + ', processing day ' + str(daynumber) + '\n'
			if (str(daynumber)!=str(d) and str(stock)=='0'):
				print "Writing size results to file"
				PerMinuteStats=db.command("collStats","StockDocPerMinute")
				PerMinuteStats.update({'Day' : str(daynumber), 'Duration' : 'perminute'})
				PerSecondStats=db.command("collStats","StockDocPerSecond")
				PerSecondStats.update({'Day' : str(daynumber), 'Duration' : 'persecond'})
				db['mystats'].insert_one(PerMinuteStats)
				db['mystats'].insert_one(PerSecondStats)

			market_hour=0
			market_minute=0
			market_second=0
			d=date.today() - timedelta(daynumber)
			for market_hour in range(0,24): #CHANGE TO 24
				print 'Worker # ' + str(stock) + ', processing day ' + str(daynumber) + ' hour + ' + str(market_hour) + '\n'
				#print 'Worker # ' + str(stock) + ', processing day ' + str(daynumber) + ' hour + ' + str(market_hour) + ' minute ' + str(market_minute) + '\n'
				#for every second
				for market_minute in range(0,60):
					p={} # this will cache a minute worth of data
					for second in range(0,60):
						#add an extra delay so numbers don't change too often
						#with lock:
						if (random.uniform(0, 1) > .9):
							newprice=getprice(last_ticker_price[stock])
							last_ticker_price[stock]=newprice
						else:
							newprice=last_ticker_price[stock]
						p.update( { str(second) : newprice } ) # { 'p' : newprice, 'v' : stockvol }})
						db['StockDocPerSecond'].insert_one({
							'symbol': symbols[stock],
							'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute, second=second),
							'p': newprice
						})
						#insert the document per event
						###REMOVE db['StockDocPerSecond'].insert_one(doc_per_event)
						#last_ticker_price[stock]=newprice
					#insert document for every one minute
					db['StockDocPerMinute'].insert_one( {
					'symbol': symbols[stock],
					'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute),
					'p' : p
					})
					###REMOVE db['StockDocPerMinute'].insert_one(doc_per_minute)
	except:
		print('Unexpected error:', sys.exc_info()[0])
		raise

main()
