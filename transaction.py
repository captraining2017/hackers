# Name		:	transaction.py
# Purpose	:	Reads transaction.csv data and predict abnormal flag based on config parameters
# Inputs	:	program reads two files 1) data file transactions.csv 
#										2) config file config.txt
# output	:	output.csv - custid,dr,cr,abnormal flag
#				fulldata.csv - transaction data after Nov8 to create monthly data

# Team 		: 	Hackers (AI team Hackathon Chennai 2017) 
# Date		:	04-Apr-2017
 

import pandas as pd
import sqlite3 as sqllite
import csv
import pandas.io.sql as sql
import ConfigParser
import matplotlib.pyplot as plt
from sklearn import model_selection
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import os
import shutil
import calendar

# section1 -- created master and transaction dataset using excel
# section2 -- configure all parameters ---
print("Program started......")
config = ConfigParser.RawConfigParser()
configFilePath = 'config.txt'
config.read(configFilePath)

try:
	datevalue = config.get('general','datevalue')
	#low
	low_cr = config.get('low','cr')
	low_dr = config.get('low','dr')
	low_maxcr = config.get('low','maxcr')
	low_maxdr = config.get('low','maxdr')
	low_depositlimit = config.get('low','depositlimit')
	low_depositpercentage = config.get('low','depositpercentage')
	#medium
	medium_cr = config.get('medium','cr')
	medium_dr = config.get('medium','dr')
	medium_maxcr = config.get('medium','maxcr')
	medium_maxdr = config.get('medium','maxdr')
	medium_depositlimit = config.get('medium','depositlimit')
	medium_depositpercentage = config.get('medium','depositpercentage')
	#high
	high_cr = config.get('high','cr')
	high_dr = config.get('high','dr')
	high_maxcr = config.get('high','maxcr')
	high_maxdr = config.get('high','maxdr')
	high_depositlimit = config.get('high','depositlimit')
	high_depositpercentage = config.get('high','depositpercentage')

except ConfigParser.NoOptionError :
    print('could not read configuration file')
    sys.exit(1) 
	
print("Loading config data completed......")
#section3 -- data munging
print("Data Munging started......")
with open('transaction.csv', 'rb') as f, sqllite.connect(':memory:') as db:
	
	#config table 
	db.execute('CREATE TABLE config(datevalue INT,low_dr INT,low_cr INT,low_maxdr INT,low_maxcr INT,low_depositlimit INT,low_depositpercentage INT,medium_dr INT,medium_cr INT,medium_maxdr INT,medium_maxcr INT,medium_depositlimit INT,medium_depositpercentage INT,high_dr INT,high_cr INT,high_maxdr INT,high_maxcr INT,high_depositlimit INT,high_depositpercentage INT)')
	cur=db.cursor()
	cur.execute('''INSERT into config(datevalue,low_dr,low_cr,low_maxdr,low_maxcr,low_depositlimit ,low_depositpercentage,medium_dr,medium_cr,medium_maxdr,medium_maxcr,medium_depositlimit ,medium_depositpercentage,high_dr,high_cr,high_maxdr,high_maxcr,high_depositlimit,high_depositpercentage) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(datevalue,low_dr,low_cr,low_maxdr,low_maxcr,low_depositlimit ,low_depositpercentage,medium_dr,medium_cr,medium_maxdr,medium_maxcr,medium_depositlimit ,medium_depositpercentage,high_dr,high_cr,high_maxdr,high_maxcr,high_depositlimit,high_depositpercentage))
	db.commit()
	
	# generating csv from sql
	table = sql.read_sql('select * from config', db)
	table.to_csv('config.csv')
	
	db.execute('CREATE TABLE fulldata(custid,custname,drcr,class,amount INT,date,status)')
	db.executemany('INSERT INTO fulldata VALUES (:custid,:custname,:drcr,:class,:amount,:date,:status)', csv.DictReader(f))
	
	#creating fulldata.csv
	table = sql.read_sql('select custid,custname,drcr,class,amount,substr(datetime(date),7,2) as date,status from fulldata where cast(date as INTEGER) > (select datevalue from config)', db)
	table.to_csv('fulldata.csv')
	
	#before november data
	db.execute('CREATE TABLE beforenov(custid,custname,class,dr,cr,tot_amount,tot_deposit)')
	cur=db.cursor()
	cur.execute('''INSERT into beforenov(custid,custname,class,tot_amount) select custid,custname,class,sum(ifnull(amount,0)) AS tot_amount from fulldata where cast(date as INTEGER) <= (select datevalue from config) group by custid,custname,class''')
	db.commit()
	
	# dr
	cursor = db.execute('select * from (SELECT count(drcr) as dr,custid,drcr FROM fulldata where cast(date as INTEGER) <= (select datevalue from config) group by custid,drcr) where drcr="0"')
	cursor = cursor.fetchall()
	for i in cursor:
		dr = i[0]
		custid = i[1]
		drcr = i[2]    
		db.execute('''UPDATE beforenov SET dr=? WHERE custid=?''', (dr, custid))
	db.commit()
	
	# cr
	cursor = db.execute('select * from (SELECT count(drcr) as cr,custid,drcr FROM fulldata where cast(date as INTEGER) <= (select datevalue from config) group by custid,drcr) where drcr="1"')
	cursor = cursor.fetchall()
	for i in cursor:
		cr = i[0]
		custid = i[1]
		drcr = i[2]    
		db.execute('''UPDATE beforenov SET cr=? WHERE custid=?''', (cr, custid))
	db.commit()
	
	#tot_deposit 
	cursor = db.execute('SELECT sum(ifnull(amount,0)) as tot_deposit,custid FROM fulldata where cast(date as INTEGER) <= (select datevalue from config) and drcr="1" group by custid')
	cursor = cursor.fetchall()
	for i in cursor:
		tot_deposit = i[0]
		custid = i[1]    
		db.execute('''UPDATE beforenov SET tot_deposit=? WHERE custid=?''', (tot_deposit, custid))
	db.commit()
	
	#after november data
	db.execute('CREATE TABLE afternov(custid,custname,class,date,dr,cr,tot_amount INT,tot_deposit INT )')
	cur=db.cursor()
	cur.execute('''INSERT into afternov(custid,custname,class,date,tot_amount) select custid,custname,class,date,sum(ifnull(amount,0)) AS tot_amount from fulldata where cast(date as INTEGER) > (select datevalue from config) group by custid,custname,class''')
	db.commit()
	
	# dr
	cursor = db.execute('select * from (SELECT count(drcr) as dr,custid,drcr FROM fulldata where cast(date as INTEGER) > (select datevalue from config) group by custid,drcr) where drcr="0"')
	cursor = cursor.fetchall()
	for i in cursor:
		dr = i[0]
		custid = i[1]
		drcr = i[2]    
		db.execute('''UPDATE afternov SET dr=? WHERE custid=?''', (dr, custid))
	db.commit()
	
	# cr
	cursor = db.execute('select * from (SELECT count(drcr) as cr,custid,drcr FROM fulldata where cast(date as INTEGER) > (select datevalue from config) group by custid,drcr) where drcr="1"')
	cursor = cursor.fetchall()
	for i in cursor:
		cr = i[0]
		custid = i[1]
		drcr = i[2]    
		db.execute('''UPDATE afternov SET cr=? WHERE custid=?''', (cr, custid))
	db.commit()
	
	#tot_deposit 
	cursor = db.execute('SELECT sum(ifnull(amount,0)) as tot_deposit,custid FROM fulldata where cast(date as INTEGER) > (select datevalue from config) and drcr="1" group by custid')
	cursor = cursor.fetchall()
	for i in cursor:
		deposit = i[0]
		custid = i[1] 
		db.execute('''UPDATE afternov SET tot_deposit=? WHERE custid=?''', (deposit, custid))
	db.commit()
	
	
	# merging before-nov and after-nov data
	db.execute('CREATE TABLE final(custid,custname,class,dr_before INT,dr_after INT ,cr_before INT,cr_after INT,totamount_after,totdeposit_after INT, dr_diff INT,cr_diff INT,deposit_diff INT,abnormal_flag)')
	cur=db.cursor()
	cur.execute('''insert into final select b.custid,b.custname,b.class,ifnull(b.dr,0) as dr_before,ifnull(a.dr,0) as dr_after,ifnull(b.cr,0) as cr_before,ifnull(a.cr,0) as cr_after,ifnull(a.tot_amount,0) as totamount_after,ifnull(a.tot_deposit,0) as totdeposit_after,ifnull((a.dr-b.dr),0) as dr_diff,ifnull((a.cr-b.cr),0) as cr_diff,ifnull((a.tot_deposit-b.tot_deposit),0) as deposit_diff,"" as abnormal_flag from beforenov b, afternov a where b.custid = a.custid''')
	db.commit()
	
	#calculating abnormal flag
	cur=db.cursor()
	cur.execute('''UPDATE final set abnormal_flag = case when class = "low" and ( cr_after > ? or totdeposit_after > ? ) then "Y" when class = "medium" and ( cr_after > ? or totdeposit_after > ? ) then "Y" when class = "high" and ( cr_after > ? or totdeposit_after > ? ) then "Y" else "N" end''',(low_maxcr,low_depositlimit,medium_maxcr,medium_depositlimit,high_maxcr,high_depositlimit))
	db.commit()
	
	# generating csv from sql
	table = sql.read_sql('select * from final', db)
	table.to_csv('output.csv')

print("Data Munging done,output.csv created") 
print("Data Analysis,Printing and Plotting started")	
#section4 -- Final data analysis,plotting etc	
dataset = pd.read_csv('output.csv')
dataset.set_index('custid')
df=dataset[['custid','cr_after','totdeposit_after','abnormal_flag']]
df.to_csv('final.csv',index=False)


# box and whisker plots
df.plot(kind='box', subplots=True, layout=(2,2), sharex=False, sharey=False)
plt.show()
# scatter diagram
pd.tools.plotting.scatter_matrix(df)
plt.show()

array = df.values
X = array[:,0:2] #0-2 columns
Y = array[:,3]   #3rd index column
validation_size = 0.90
seed = 4
X_train, X_validation, Y_train, Y_validation = model_selection.train_test_split(X, Y, test_size=validation_size, random_state=seed)

# Make predictions on validation dataset
knn = KNeighborsClassifier()
knn.fit(X_train, Y_train)
predictions = knn.predict(X_validation)
print(accuracy_score(Y_validation, predictions))
print(confusion_matrix(Y_validation, predictions))
print(classification_report(Y_validation, predictions))

#section5 - Feed dataset,polling folder,creating dataset for month started
print("Feed dataset,polling folder,creating dataset for month started....")
fulldata = pd.read_csv('fulldata.csv')
for idx in range(1, 13):
	month=fulldata[fulldata.date==idx]
	if len(month.index) > 0:
		folder=calendar.month_abbr[idx]
		if os.path.isdir(folder):
			shutil.rmtree(folder)
		os.makedirs(folder)
		file="./"+folder+"/"+folder+".csv"
		month.to_csv(file,index=False)
print("Feed dataset,polling folder,creating dataset for month ended....")

#cleaning -- if user want below csv for more analysis plz uncomment
if os.path.exists('fulldata.csv'):
	os.remove('fulldata.csv')

if os.path.exists('output.csv'):
	os.remove('output.csv')

if os.path.exists('config.csv'):
	os.remove('config.csv')

print("Program Ended............")
	