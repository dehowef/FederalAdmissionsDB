import os
import psycopg2
import csv

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	print "Connection Success"
except:
	print "Connection Unsuccessful"




conn.close()
