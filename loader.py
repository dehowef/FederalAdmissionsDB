import os
import psycopg2
import csv
import sys

def isREAL(txt):
	try:
		float(txt)
		return True
	except  ValueError:
		return False

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
#create table		
		sql = "CREATE TABLE IF NOT EXISTS " + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' FLOAT, \n'
		sql += row[-1] + ' FLOAT\n); '
		sql = str(sql)
		print sql
		cur.execute(sql)

#load table
	print "Now loading table " + tablename
	with (open(filename, "rb")) as f:
		reader = csv.reader(f)
		row = reader.next()
		x = 0
		for row in reader:
			if(x == 0):
				sql = "INSERT INTO " + tablename + " VALUES "
			sql += "(" + row[0] + ', '

			for value in row[1:-1]:
				if isREAL(value):	
					sql += value + ', '
				else:
					sql += "66666" + ', '
			
			if isREAL(row[-1]):
				sql += row[-1] + ")"
			else:
				sql += "66666" + ")"
			
			if(x != 1000):
				sql += ", \n"

			if(x == 1000):
				sql += "\n ;"
				sql = str(sql) #need a cast.
				#print "value of x is ", x
				#execute here.
				cur.execute(sql)
				sql = "" 
				print "Insertion"
				x = 0
				continue
			x += 1

		if sql != "": 
			sql = str(sql)[:-3]
			lastinsert = "INSERT INTO " + tablename + " VALUES " + sql + " ;" 
			cur.execute(sql)
			print "final value of x is ", x
			#insert here.

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

#create table
	with (open(filename)) as f:
		reader = csv.reader(f)
		row = reader.next()
		
		sql = "CREATE TABLE IF NOT EXISTS" + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' VARCHAR(100), \n'
		sql += row[-1] + ' VARCHAR(100)\n); '
		#print sql

#load table

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	cur = conn.cursor()
	
	print "Connection Success"
except:
	print "Connection Unsuccessful"


buildNTHS("PERV2PUB.CSV")
#buildNTHS("VEHV2PUB.CSV")
#buildEIA("EIA_CO2_Electricity_2015.csv")
# buildEIA("ERROR.CSV")
cur.close()
conn.close()
