import snap7
import time
import threading
import subprocess
import struct
from snap7 import util
import mysql.connector
mydb = mysql.connector.connect( 
  host="host_ip_address", 
  user="user_name", 
  password="password", 
  database="database_name",
  use_pure=True
)
table="table_name";
cycle_id=0;
time.sleep(10)
sample_time=5
snapclient=snap7.client.Client()

mycursor = mydb.cursor()


dbs={
    

237:{
        0:{
            "name":"actual_speed",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":2,
            "lower_tolarence":2,
            "current_value":0,
            "condition_to_log_value":0,
            "condition_to_log":0,
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            },
        4:{
            "name":"actual_length",
            "type":"db_dint",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":50,
            "lower_tolarence":50,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"asc_cycle_id"
            }
        }

}

cycle_id=0  
#Siemens datatypes
    datatypes={"db_real":4,"db_int":2,"db_dint":4,"word":2,"int":2,"bool":1}


mycursor.execute("SELECT cycle_id FROM "+table+" ORDER BY cycle_id DESC LIMIT 0,1")
record = mycursor.fetchone()
if record is not None:
    cycle_id=int(record[0])
else:
    cycle_id=1

def asc_cycle_id(new_value):
    global dbs,cycle_id
    if (new_value<dbs[237][4]["current_value"]):
        cycle_id+=1
        for db in dbs:
            for data in dbs[db]:
                dbs[db][data]["current_value"]=0
        
def empty_func(a):
    1
def get_dint(_bytearray, byte_index):
    """
    Get int value from bytearray.

    double int are represented in four bytes
    """
    byte3 = _bytearray[byte_index + 3]
    byte2 = _bytearray[byte_index + 2]
    byte1 = _bytearray[byte_index + 1]
    byte0 = _bytearray[byte_index]
    return byte3 + (byte2 << 8) + (byte1 << 16) + (byte0 << 32)

def send_data(cycle_id,data_name,data_value,storing_data_type):
    global mycursor,mydb,table
    
    if storing_data_type=="int":
        data_value=int(data_value)
        
    # this section is needed only at the first run    
    sql="ALTER TABLE "+table+" ADD COLUMN IF NOT EXISTS "+data_name+" "+storing_data_type
    mycursor.execute(sql)
    mydb.commit()    
    
    sql="INSERT INTO "+table+" (cycle_id,"+data_name+") VALUES (%s,%s)"
    val=(cycle_id,data_value)
    try:    
        mycursor.execute(sql,val)
        mydb.commit()
    except mysql.connector.Error as err:
        print("Something went wrong: {}".format(err))
    print(cycle_id,data_name,data_value)



while True: 
    while str(snapclient.get_cpu_state()) != "S7CpuStatusRun":
            try:
                plc_rack=0
                plc_slot=0
                plc_ip="192.168.0.1"
                snapclient.connect(plc_ip,plc_rack,plc_slot)
                
            except:
                subprocess.run(["service", "bencenek_adat","restart"])
            continue 
        
        
    for db in dbs:
    #without the following delay you can have "connection timeout" error on older PLCs        
            time.sleep(0.1) 
            for data in dbs[db]:
                #print(dbs[db][data]["name"])
                if int(db)>0:
                    get_curr_data=snapclient.db_read(int(db),int(data),datatypes[dbs[db][data]["type"]])
                elif int(db)==0:
                    get_curr_data=snapclient.read_area(snap7.types.Areas.MK,0, int(data), datatypes[dbs[db][data]["type"]])
        
                if dbs[db][data]["type"]=="db_real":
                    current_value=round(snap7.util.get_real(get_curr_data,0),dbs[db][data]["precision"])
                elif dbs[db][data]["type"]=="db_int":
                    current_value=round(snap7.util.get_int(get_curr_data,0),dbs[db][data]["precision"])
                elif dbs[db][data]["type"]=="db_dint":
                    current_value=round(get_dint(get_curr_data,0),dbs[db][data]["precision"])
                elif dbs[db][data]["type"]=="int":
                    current_value=round(struct.unpack('!H', get_curr_data)[0]*dbs[db][data]["rescale"],dbs[db][data]["precision"])
                elif dbs[db][data]["type"]=="bool":
                    current_value=snap7.util.get_bool(get_curr_data,0,int(10*float(data)-10*int(data)))   
                if current_value!=dbs[db][data]["current_value"]:
                    #diff=abs(dbs[db][data]["current_value"]-current_value)
                 #   print("---------------------")
                    print(dbs[db][data]["name"],"actual:",current_value)
                  #  print("---------------------")
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
                        globals()[dbs[db][data]["function_on_changing"]](current_value)
                        send_data(cycle_id,dbs[db][data]["name"],current_value,dbs[db][data]["storing_data_type"])
                        dbs[db][data]["current_value"]=current_value
        
                
    time.sleep(sample_time)
    
 
