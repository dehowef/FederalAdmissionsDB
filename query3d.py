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

months = [200803,200804,200805,200806,200807,200808,200809,200810,200811,200812,200901,200902,200903,200904]

########################################### QUERY 3D ########################################

for month in months:
	#get number of households surveyed in month X
	num_house_subquery = "SELECT COUNT(DISTINCT(HOUSEID)) FROM DAY WHERE VEHID >= 1 AND TRPMILES > 0 AND TDAYDATE =" 
	cur.execute(num_house_subquery + str(month))
	households = cur.fetchall()
	num_households = households[0][0]
	#print "number houses: " + str(num_households)
	house_scale = float(117538000/num_households)

	#get the total amount of c02 emissions from gasoline from vehicles for one day in month X (don't multiply by num of days yet)
	gasoline_c02_query = "SELECT SUM(((1.0*TRPMILES)/EPATMPG)*.008887) FROM DAY NATURAL JOIN VEHICLE WHERE VEHID >=1 AND TRPMILES > 0 AND TDAYDATE ="
	cur.execute(gasoline_c02_query + str(month))
	gasoline_c02 = cur.fetchall()

	total_c02 = gasoline_c02[0][0]*31*house_scale
	#print "total c02 before scale: " + str(gasoline_c02[0][0]*31)
	print "total c02 from normal vehicles: " + str(total_c02)

	#get conversion for KWH -> CO2
	c02_ratio_query = "SELECT ELECTRICITY.VALUE, MKWH.VALUE FROM ELECTRICITY,MKWH WHERE MKWH.MSN='ELETPUS' AND ELECTRICITY.MSN='TXEIEUS' AND ELECTRICITY.YYYYMM = "
	cur.execute(c02_ratio_query + "'" + str(month) + "'" + " AND MKWH.YYYYMM='" + str(month) + "'")
	c02_ratio = cur.fetchall()
	ratio = float(c02_ratio[0][0])/float(c02_ratio[0][1])

	#get total kwh from hybrids who only drive in their electricity range
	hybrids_drive_20_query = "SELECT SUM(miles/(EPATMPG*0.09063441)) FROM (SELECT SUM(TRPMILES) as miles,EPATMPG FROM DAY NATURAL JOIN VEHICLE WHERE VEHID>=1 AND TRPMILES>0 AND TDAYDATE="
	cur.execute(hybrids_drive_20_query + str(month) + " GROUP BY HOUSEID,VEHID,EPATMPG)sub WHERE miles <=20")
	sum_result = cur.fetchall()
	hybrids_20 = sum_result[0][0]*31*house_scale*ratio
	#print "c02 from vehicles who only drive electric: " + str(hybrids_20)

	#get total kwh and gas used from hybrids who drive more than their electricity range
	hybrids_drive_more_query = "SELECT SUM(20.0/(EPATMPG*0.09063441)), SUM(((totalmiles-20)/EPATMPG)*.008887) FROM (SELECT SUM(TRPMILES) totalmiles, EPATMPG FROM DAY NATURAL JOIN VEHICLE WHERE VEHID>=1 AND TRPMILES>0 AND TDAYDATE= " 
	cur.execute(hybrids_drive_more_query + str(month) + " GROUP BY HOUSEID,VEHID,EPATMPG)trip WHERE totalmiles >20")
	kwh_gas_result = cur.fetchall()

	kwh_electricity = kwh_gas_result[0][0]*31*house_scale*ratio
	#print "c02 from first 20 miles: " + str(kwh_electricity)
	c02_gas = kwh_gas_result[0][1]*31*house_scale
	#print "c02 from further miles: " + str(c02_gas)
	hybrids_drive_more_20 = kwh_electricity+c02_gas
	#print "total c02 from those miles: " + str(hybrids_drive_more_20)

	#get total C02 emissions from electricity
	c02_from_hybrids = hybrids_20 + hybrids_drive_more_20
	print "total c02 from hybrids: " + str(c02_from_hybrids)
	#print "total c02 from hybrids before scale: " + str((gasoline_c02[0][0] + kwh_gas_result[0][0] + kwh_gas_result[0][1]))
	# subtract electricity + remaining co2 emissions using gas from total co2 emissions using 100% gasoline
	change_in_c02 = total_c02 - c02_from_hybrids
	print "For " + str(month) + " change in c02 if all vehicles were hybrids: " + str(change_in_c02)
 
conn.close()
