import csv
import os
import psycopg2

conn = psycopg2.connect(database="postgres", host= "/home/" + os.environ['USER'] + "/postgres")
print "Opened db successfully"

cur = conn.cursor();
##############
#  QUERY 3A  #
##############

#get number of days in month to calculate number of individuals making a trip
def days_in_month(month):
	if (month == "04" or month == "06" or month == "09" or month == "11"):
		days = 30
	elif (month == "02"):
		days = 28
	else:
		days = 31
	return days

def query3A(): 
	#get the traveldaydate of all individuals who travel less than 100 miles a day
	query_total = "SELECT TDAYDATE FROM (SELECT SUM(TRPMILES) AS miles, HOUSEID, PERSONID, TDAYDATE FROM DAY GROUP BY HOUSEID,PERSONID,TDAYDATE) Trip WHERE Trip.miles < 100"
	cur.execute(query_total);
	totalnum = cur.fetchall()

	#calculate the total number of individuals who travel less than 100 miles a day
	individuals_less_than_100 = 0
	for num in totalnum:
		month = str(num[0])[4:6]
		individuals_less_than_100 += days_in_month(month) #will hold the total num of individuals who travel less than 100 mpd at the end of loop
	print individuals_less_than_100
	
	#get the individuals who travel less than X miles
	query = "SELECT TDAYDATE, miles FROM (SELECT SUM(TRPMILES) AS miles,HOUSEID,PERSONID,TDAYDATE FROM DAY GROUP BY HOUSEID,PERSONID,TDAYDATE) Trip WHERE Trip.miles < "
	
	mile = 5
	while (mile <= 100):
		cur.execute(query + str(mile))
		result = cur.fetchall() #the TDAYDATE and miles of everyone who travels less than X miles. each tuple represents ONE individual
		num_people = 0
		for person in result:
			month = str(person[0])[4:6] #parsing out the month in TDAYDATE
			num_people += days_in_month(month) #loop until get the total num of individuals, assuming they travel the same num of miles each day of that month
		print str(num_people) + " travel less than " + str(mile) + " miles a day"
		print str("%.2f" % (num_people/float(individuals_less_than_100)*100)) + "% of people travel less than " + str(mile) + " miles a day"	
		mile += 5

##############
#  QUERY 3B  #
##############
def query3B():
	for i in range (5, 101, 5):
		sql =  "SELECT AVG(EPATMPG) FROM VEHICLE INNER JOIN DAY ON DAY.HOUSEID" 
		sql += "= VEHICLE.HOUSEID WHERE VEHICLE.VEHID >= 1 AND DAY.TRPMILES < %d;" % i
		print sql
		cur.execute(sql)
		print "Average EPATMPG for trips less than",i,"is", cur.fetchone()[0]

	

query3B()	
conn.close()
