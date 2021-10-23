import time
import snap7
from snap7 import util
from snap7.snap7exceptions import Snap7Exception
client=snap7.client.Client()
import mysql.connector
import my_config

cycle_id=0  

cursor = mydb.cursor()
cursor.execute("SELECT cycle_id FROM "+table+" ORDER BY cycle_id DESC LIMIT 0,1")
record = cursor.fetchone()
if record is not None:
    cycle_id=int(record[0])
else:
    cycle_id=1
    
def empty_func(a):
    1
        
def send_data(cycle_id,data_name,data_value,storing_data_type):
    global mydb,table
    mycursor=mydb.cursor()
    if storing_data_type=="integer":
        data_value=int(data_value)
    sql="INSERT INTO "+table+" (cycle_id,"+data_name+") VALUES (%s,%s)"
    val=(cycle_id,data_value)
    mycursor.execute(sql,val)
    mydb.commit()
    print(cycle_id,data_name,data_value)

def asc_cycle_id(new_value):
    global dbs,cycle_id
    if (new_value==0 and dbs[14][20]["current_value"]>0):
        cycle_id+=1
        for db in dbs:
            for data in dbs[db]:
                dbs[db][data]["current_value"]=0


while True: 
    while client.get_connected() == False:
        try:
            client.connect(plc_ip,plc_rack,plc_slot)
            
        except Snap7Exception as e:
            time.sleep(10)
            continue
        
        
    for db in dbs:
        for data in dbs[db]:
            get_curr_data=client.db_read(db,data,datatypes[dbs[db][data]["type"]])
            if dbs[db][data]["type"]=="real":
                current_value=round(util.get_real(get_curr_data,0),dbs[db][data]["precision"])
                if current_value!=dbs[db][data]["current_value"]:
                    globals()[dbs[db][data]["function_on_changing"]](current_value)
                    #diff=abs(dbs[db][data]["current_value"]-current_value)
                    print(dbs[db][data]["name"])
                    print(" current ")
                    print(current_value)
                    print(dbs[db][data]["current_value"])
                    print("---------------------")
                    send=False
                    if (dbs[db][data]["condition_to_log"]==0 and (dbs[db][data]["upper_tolarence"]+dbs[db][data]["current_value"]<current_value or dbs[db][data]["current_value"]-dbs[db][data]["lower_tolarence"]>current_value)):
                        #no condition and the current value is out of tolerance
                        send=True
                    elif dbs[db][data]["condition_to_log"]==">" and (dbs[dbs[db][data]["condition_to_log_value"][0]][dbs[db][data]["condition_to_log_value"][1]]["current_value"]>dbs[db][data]["condition_to_log_compare"] and (dbs[db][data]["current_value"]+dbs[db][data]["upper_tolarence"]<current_value or dbs[db][data]["current_value"]-dbs[db][data]["lower_tolarence"]>current_value)):
                        send=True
                    elif dbs[db][data]["condition_to_log"]=="<" and (dbs[dbs[db][data]["condition_to_log_value"][0]][dbs[db][data]["condition_to_log_value"][1]]["current_value"]<dbs[db][data]["condition_to_log_compare"] and (dbs[db][data]["current_value"]+dbs[db][data]["upper_tolarence"]<current_value or dbs[db][data]["current_value"]-dbs[db][data]["lower_tolarence"]>current_value)):
                        send=True
                    elif dbs[db][data]["condition_to_log"]=="=" and (dbs[dbs[db][data]["condition_to_log_value"][0]][dbs[db][data]["condition_to_log_value"][1]]["current_value"]==dbs[db][data]["condition_to_log_compare"] and (dbs[db][data]["current_value"]+dbs[db][data]["upper_tolarence"]<current_value or dbs[db][data]["current_value"]-dbs[db][data]["lower_tolarence"]>current_value)):
                        send=True
                    elif dbs[db][data]["current_value"]==0:
                        send=True
                    if send==True:
                        send_data(cycle_id,dbs[db][data]["name"],current_value,dbs[db][data]["storing_data_type"])
                        dbs[db][data]["current_value"]=current_value
                
    time.sleep(sample_time)
