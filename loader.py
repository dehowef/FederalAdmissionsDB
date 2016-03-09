import os
import psycopg2
import csv
import sys

def buildNTHS(filename):
	
	if 'PER' in filename:
		tablename = 'PERSON'
	elif 'VEH' in filename:
		tablename = 'VEHICLE'
	elif 'DAY' in filename:
		tablename = 'DAY'
	elif 'HHV2' in filename:
		tablename = 'HOUSEHOLD'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	with (open(filename)) as f:
		reader = csv.reader(f)
		row = reader.next()
		
		sql = "CREATE TABLE " + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' INT, \n'
		sql += row[-1] + ' INT\n); '
		print sql

def buildEIA(filename):

	if 'Electricity' in filename:
		tablename = 'ELECTRICITY'
	elif 'VEH' in filename:
		tablename = 'TRANSPORTATION'
	elif 'MkWh' in filename:
		tablename = 'MKWH'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	with (open(filename)) as f:
		reader = csv.reader(f)
		row = reader.next()
		
		sql = "CREATE TABLE " + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' VARCHAR(100), \n'
		sql += row[-1] + ' VARCHAR(100)\n); '
		print sql

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	print "Connection Success"
except:
	print "Connection Unsuccessful"

"""with (open('PERV2PUB.CSV')) as f:
	reader = csv.reader(f)
	row = reader.next()
	
	sql = "CREATE TABLE PERV2 \n(\n" 
	for value in row[:-1]:
		sql += value + ' INT, \n'
	sql += row[-1] + ' INT\n); '
"""

buildNTHS("PERV2PUB.CSV")
buildNTHS("VEHV2PUB.CSV")
buildEIA("EIA_CO2_Electricity_2015.csv")
buildEIA("ERROR.CSV")

conn.close()
