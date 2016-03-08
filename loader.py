import os
import psycopg2
import csv

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	print "Connection Success"
except:
	print "Connection Unsuccessful"

with (open('PERV2PUB.CSV')) as f:
	reader = csv.reader(f)
	row = reader.next()
	
	sql = "CREATE TABLE PERV2 \n(\n" 
	for value in row[:-1]:
		sql += value + ' INT, \n'
	sql += row[-1] + ' INT\n); '

print sql
conn.close()
