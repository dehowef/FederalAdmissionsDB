import psycopg2
import os
import csv


conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
print "Opened db successfully"

cur = conn.cursor()

def loaddata(insert, tuplelist):
	cur.executemany(insert, tuplelist)

tup = ()
tuplelist = [] 
table = []
insert = []
datafile = []
datafile.append('HHV2PUB.CSV')
#datafile.append('DAYV2PUB.CSV')
#datafile.append('PERV2PUB.CSV')
#datafile.append('VEHV2PUB.CSV')

table.append("CREATE TABLE IF NOT EXISTS Household(HOUSEID INT PRIMARY KEY, VARSTRAT INT, WTHHFIN NUMERIC, DRVRCNT INT, CDIVMSAR INT, CENSUS_D INT, CENSUS_R INT, HH_HISP INT, HH_RACE INT, HHFAMINC INT, HHRELATD INT, HHRESP INT, HHSIZE INT, HHSTATE VARCHAR(2), HHSTFIPS INT, HHVEHCNT INT, HOMEOWN INT, HOMETYPE INT, MSACAT INT, MSASIZE INT, NUMADLT INT, RAIL INT, RESP_CNT INT, SCRESP INT, TRAVDAY INT, URBAN INT, URBANSIZE INT, URBRUR INT, WRKCOUNT INT, TDAYDATE INT, FLAG100 INT, LIF_CYC INT, CNTTDHH INT, HBHUR VARCHAR(2), HTRESDN INT, HTHTNRNT INT, HTPPOPDN INT, HTEEMPDN INT, HBRESDN INT, HBHTNRNT INT, HBPPOPDN INT, HH_CBSA VARCHAR(5), HHC_MSA VARCHAR(4))")

#table.append("CREATE TABLE IF NOT EXISTS DayTrip(HOUSEID INT,PERSONID INT,FRSTHM INT,OUTOFTWN INT,ONTD_P1 INT,ONTD_P2 INT,ONTD_P3 INT,ONTD_P4 INT,ONTD_P5 INT,ONTD_P6 INT,ONTD_P7 INT,ONTD_P8 INT,ONTD_P9 INT,ONTD_P10 INT,ONTD_P11 INT,ONTD_P12 INT,ONTD_P13 INT,ONTD_P14 INT,ONTD_P15 INT,TDCASEID BIGINT,HH_HISP INT,HH_RACE INT,DRIVER INT,R_SEX INT,WORKER INT,DRVRCNT INT,HHFAMINC INT,HHSIZE INT,HHVEHCNT INT,NUMADLT INT,FLAG100 INT,LIF_CYC INT,TRIPPURP VARCHAR(8),AWAYHOME INT,CDIVMSAR INT,CENSUS_D INT,CENSUS_R INT,DROP_PRK INT,DRVR_FLG INT,EDUC INT,ENDTIME INT,HH_ONTD INT,HHMEMDRV INT,HHRESP INT,HHSTATE VARCHAR(2),HHSTFIPS INT,INTSTATE INT,MSACAT INT,MSASIZE INT,NONHHCNT INT,NUMONTRP INT,PAYTOLL INT,PRMACT INT,PROXY INT,PSGR_FLG INT,R_AGE INT,RAIL INT,STRTTIME INT,TRACC1 INT,TRACC2 INT,TRACC3 INT,TRACC4 INT,TRACC5 INT,TRACCTM INT,TRAVDAY INT,TREGR1 INT,TREGR2 INT,TREGR3 INT,TREGR4 INT,TREGR5 INT,TREGRTM INT,TRPACCMP INT,TRPHHACC INT,TRPHHVEH INT,TRPTRANS INT,TRVL_MIN INT,TRVLCMIN INT,TRWAITTM INT,URBAN INT,URBANSIZE INT,URBRUR INT,USEINTST INT,USEPUBTR INT,VEHID INT,WHODROVE INT,WHYFROM INT,WHYTO INT,WHYTRP1S INT,WRKCOUNT INT,DWELTIME INT,WHYTRP90 INT,TDTRPNUM INT,TDWKND INT,TDAYDATE INT,TRPMILES NUMERIC,WTTRDFIN NUMERIC,VMT_MILE NUMERIC,PUBTRANS INT,HOMEOWN INT,HOMETYPE INT,HBHUR VARCHAR(2),HTRESDN INT,HTHTNRNT INT,HTPPOPDN INT,HTEEMPDN INT,HBRESDN INT,HBHTNRNT INT,HBPPOPDN INT,GASPRICE NUMERIC,VEHTYPE INT,HH_CBSA VARCHAR(5),HHC_MSA VARCHAR(4), PRIMARY KEY(HOUSEID, PERSONID, TDTRPNUM))")

#table.append("CREATE TABLE IF NOT EXISTS Person(HOUSEID INT,PERSONID INT,VARSTRAT INT,WTPERFIN NUMERIC,SFWGT INT,HH_HISP INT,HH_RACE INT,DRVRCNT INT,HHFAMINC INT,HHSIZE INT,HHVEHCNT INT,NUMADLT INT,WRKCOUNT INT,FLAG100 INT,LIF_CYC INT,CNTTDTR INT,BORNINUS INT,CARRODE INT,CDIVMSAR INT,CENSUS_D INT,CENSUS_R INT,CONDNIGH INT,CONDPUB INT,CONDRIDE INT,CONDRIVE INT,CONDSPEC INT,CONDTAX INT,CONDTRAV INT,DELIVER INT,DIARY INT,DISTTOSC INT,DRIVER INT,DTACDT INT,DTCONJ INT,DTCOST INT,DTRAGE INT,DTRAN INT,DTWALK INT,EDUC INT,EVERDROV INT,FLEXTIME INT,FMSCSIZE INT,FRSTHM INT,FXDWKPL INT,GCDWORK INT,GRADE INT,GT1JBLWK INT,HHRESP INT,HHSTATE VARCHAR(2),HHSTFIPS INT,ISSUE INT,OCCAT INT,LSTTRDAY INT,MCUSED INT,MEDCOND INT,MEDCOND6 INT,MOROFTEN INT,MSACAT INT,MSASIZE INT,NBIKETRP INT,NWALKTRP INT,OUTCNTRY INT,OUTOFTWN INT,PAYPROF INT,PRMACT INT,PROXY INT,PTUSED INT,PURCHASE INT,R_AGE INT,R_RELAT INT,R_SEX INT,RAIL INT,SAMEPLC INT,SCHCARE INT,SCHCRIM INT,SCHDIST INT,SCHSPD INT,SCHTRAF INT,SCHTRN1 INT,SCHTRN2 INT,SCHTYP INT,SCHWTHR INT,SELF_EMP INT,TIMETOSC INT,TIMETOWK INT,TOSCSIZE INT,TRAVDAY INT,URBAN INT,URBANSIZE INT,URBRUR INT,USEINTST INT,USEPUBTR INT,WEBUSE INT,WKFMHMXX INT,WKFTPT INT,WKRMHM INT,WKSTFIPS INT,WORKER INT,WRKTIME INT,WRKTRANS INT,YEARMILE INT,YRMLCAP INT,YRTOUS INT,DISTTOWK INT,TDAYDATE INT,HOMEOWN INT,HOMETYPE INT,HBHUR VARCHAR(2),HTRESDN INT,HTHTNRNT INT,HTPPOPDN INT,HTEEMPDN INT,HBRESDN INT,HBHTNRNT INT,HBPPOPDN INT,HH_CBSA VARCHAR(5),HHC_MSA VARCHAR(4), PRIMARY KEY(HOUSEID, PERSONID))")

#table.append("CREATE TABLE IF NOT EXISTS Vehicle(HOUSEID INT,WTHHFIN NUMERIC,VEHID INT,DRVRCNT INT,HHFAMINC INT,HHSIZE INT,HHVEHCNT INT,NUMADLT INT,FLAG100 INT,CDIVMSAR INT,CENSUS_D INT,CENSUS_R INT,HHSTATE VARCHAR(2),HHSTFIPS INT,HYBRID INT,MAKECODE INT,MODLCODE INT,MSACAT INT,MSASIZE INT,OD_READ INT,RAIL INT,TRAVDAY INT,URBAN INT,URBANSIZE INT,URBRUR INT,VEHCOMM INT,VEHOWNMO INT,VEHYEAR INT,WHOMAIN INT,WRKCOUNT INT,TDAYDATE INT,VEHAGE INT,PERSONID INT,HH_HISP INT,HH_RACE INT,HOMEOWN INT,HOMETYPE INT,LIF_CYC INT,ANNMILES INT,HBHUR VARCHAR(2),HTRESDN INT,HTHTNRNT INT,HTPPOPDN INT,HTEEMPDN INT,HBRESDN INT,HBHTNRNT INT,HBPPOPDN INT,BEST_FLG INT,BESTMILE NUMERIC,BEST_EDT INT,BEST_OUT INT,FUELTYPE INT,GSYRGAL INT,GSCOST NUMERIC,GSTOTCST INT,EPATMPG NUMERIC,EPATMPGF INT,EIADMPG NUMERIC,VEHTYPE INT,HH_CBSA VARCHAR(5),HHC_MSA VARCHAR(4), PRIMARY KEY(HOUSEID, VEHID))")

insert.append("INSERT INTO Household VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
  
for t in table:
	cur.execute(t)
	print "created table"
i = 0

for data in datafile:
	with open(data, "rb") as f:
		reader = csv.reader(f, delimiter=",")
		x = 0;
		for line in reader:
			if (x % 1000 == 0):
				#call executemany(tuplelist)
				loaddata(insert[i],tuplelist)
				tuplelist = []
			tup = tuple(line)
			tuplelist.append(tup)
			x += 1
	i+=1

