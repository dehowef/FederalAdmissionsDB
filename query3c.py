import csv
import os
import psycopg2

conn = psycopg2.connect(database="postgres", host= "/home/" + os.environ['USER'] + "/postgres")
print "Opened db successfully"

cur = conn.cursor();

#get number of days in month to calculate number of individuals making a trip
def days_in_month(month):
	if (month == "04" or month == "06" or month == "09" or month == "11"):
		days = 30
	elif (month == "02"):
		days = 28
	else:
		days = 31
	return days

#get the total amount of c02 emissions from every household in month X 
sum_subquery = "SELECT SUM((1.0 * TRPMILES)/(1.0*EPATMPG)*.008883) FROM (SELECT * FROM DAY NATURAL JOIN VEHICLE WHERE VEHID >=1 AND TRPMILES > 0 AND TDAYDATE ="

#get the total amount of c02 emissions of transportation sector for a specific month
total_c02_subquery = "SELECT VALUE FROM TRANSPORTATION WHERE MSN = 'TEACEUS' AND YYYYMM ="

#get number of households surveyed in month X
num_house_subquery = "SELECT COUNT(DISTINCT(HOUSEID)) FROM DAY WHERE VEHID >= 1 AND TRPMILES > 0 AND TDAYDATE ="

#months of survey
months = [200803,200804,200805,200806,200807,200808,200809,200810,200811,200812,200901,200902,200903,200904]

for month in months:
	#sum of c02 emissions
	cur.execute(sum_subquery + str(month) + ") AS day_join_v")
	month_sum_c02 = cur.fetchall()

	#num of households
	cur.execute(num_house_subquery + str(month))
	households = cur.fetchall()

	#total c02 emissions from EIA data
	cur.execute(total_c02_subquery + "'" + str(month) + "'")
	c02 = cur.fetchall()

	#extract from tuples
	month_co2_emission = month_sum_c02[0][0]*days_in_month(month)
	num_households = households[0][0]
	total_c02 = c02[0][0]
	
	print "For " + str(month) + ":"

	house_scale = 117538000/num_households
	scaled_co2_emission = (month_co2_emission * house_scale)/(float(total_c02)*1000000) * 100
	
	print "Percentage of c02 emissions from household vehicles: " + str("%.2f" % scaled_co2_emission) + "%"
conn.close()
