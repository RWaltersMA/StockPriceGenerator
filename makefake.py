import random
import csv
import argparse
import pymongo
#from datetime import datetime, timedelta as dt
from datetime import date, timedelta
from datetime import datetime as dt

MONGO_URI = 'mongodb://localhost:27020/Stock'

volatility=.01
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
			f.seek(0) # we counted the lines need to rewind
			f.next() # skip the header row
			reader = csv.reader(f, delimiter="|")
			for i, line in enumerate(reader,1):
				#print 'line[{}] = {}'.format(i, line)
				if (i<row_count):
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
	parser.add_argument("-v", help="maximum volume per day")
	
	args = parser.parse_args()
	if (int(args.v)):
		print("You need to specify a volume greater than 1000")
		exit
	#fill the symbols array with the ticker symbols we will fill our data with
	processfile(args.f)
	c = pymongo.MongoClient(MONGO_URI)
	db = c.get_database(name='Stock')
	db['StockDocPerSecond'].drop()
	db['StockDocPerMinute'].drop()
	d_from = date.today() - timedelta(int(args.d))
	print("Creating sample data starting from " + str(d_from) + " to today (" + args.d + " days)")
	for daynumber in range(int(args.d),0,-1):
		#print("Processing Day " + str(daynumber) )
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
	
		#for each day how much volume should there be?
		day_volume=random.randint(10000000, int(args.v))
		market_hour=0
		market_minute=0
		#market_second=0
		while day_volume>0:
			#randomly pick a stock
			stockpick=random.randint(0,len(symbols)-1)
			#randomly pick number of shares traded
			#stockvol=random.randint(100,10000)
			#newprice=getprice(last_ticker_price[stockpick])
			##print("Day " + str(daynumber) + " : " + symbols[stockpick] + " volume: " + str(day_volume)) # + " - " + str(newprice) + " @ " + str(stockvol))
			#insert as document per event
			d=date.today() - timedelta(daynumber)
			#create a minutes worth of data for the given stock
			#save to the database for each tick - StockDocPerValue
			#queue up the entire minute and submit one doc per min - StockDocPerMin
			doc_per_minute = {
				'symbol': symbols[stockpick],
				'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute)
			}
			p={} # this will cache a minute worth of data
	
			for second in range(0,59):
				stockvol=random.randint(100,10000)
				newprice=getprice(last_ticker_price[stockpick])
				p.update( { str(second) : newprice } ) # { 'p' : newprice, 'v' : stockvol }})
				doc_per_event = {
					'symbol': symbols[stockpick],
					'd': dt(year=d.year, month=d.month, day=d.day, hour=market_hour, minute=market_minute, second=second),
					'p': newprice#,
					#'v': stockvol
					}
				db['StockDocPerSecond'].insert_one(doc_per_event)
				last_ticker_price[stockpick]=newprice
				day_volume=day_volume-stockvol
			#determine when we should increase time
			#market_second=market_second+1
			#if (market_second>59):
		#		market_second=0
			doc_per_minute.update({'p' : p})
			db['StockDocPerMinute'].insert_one(doc_per_minute)
			market_minute=market_minute+1
			if (market_minute>59):
				market_minute=0
				market_hour=market_hour+1
			#day_volume=day_volume-stockvol
			if (market_hour>23):
				break
	

main()
# {
# attributes:
# symbol: 
# price: 
# }
# event granulatory - writes document per event, document per minute, per hour
