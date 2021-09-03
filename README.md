# StockPriceGenerator

This tool will populate a MongoDB database with fake stock price data.  Users can specify how many random stocks to generate and for how long


Usage: (generate an hour of datra for 5 companies written to the local MongoDB database)

python3 stockgen.py -s 5 -x 60

-s the number of company symbols
-c MongoDB Connection string
-d Destination database name
-col Destination collection name
-x Number of minutes of data to generate (default 0 = forever)
	
