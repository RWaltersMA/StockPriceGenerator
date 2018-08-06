# StockPriceGenerator

This tool will populate a MongoDB database with fake stock price data


Usage:

python stockgen.py -f stocklist.txt -d 10

-f is a text file that contains the list of stocks 
-d is the number of days of data you want to create

The StockGen tool used to generate sample data will generate the same data and store it in two different collections: StockDocPerSecond and StockDocPerMinute.  This tool is used to showcase how different schema designs for the same data can effect data and index size in MongoDB.
