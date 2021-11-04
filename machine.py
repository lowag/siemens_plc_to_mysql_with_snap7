import time
import snap7
import struct
from snap7 import util
from snap7.snap7exceptions import Snap7Exception
client=snap7.client.Client()
import mysql.connector
area = snap7.snap7types.areas.MK
mydb = mysql.connector.connect( 
  host="*******", 
  user="******", 
  password="*************", 
  database="*************",
  use_pure=True
)
plc_rack=0
plc_slot=0
plc_ip="*****"
sample_time=10



table="******"

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
            "precision":-1,
            "upper_tolarence":50,
            "lower_tolarence":50,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"asc_cycle_id"
            }
    
        },
     11:{
        12:{
            "name":"speed_setpoint",
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
            }
    
        },    
   0:{#MW244
       244:{
            "name":"temp_after_dryer4",
            "type":"int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "rescale":0.01,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            }
    
        },    
    270:{
        24:{
            "name":"burner1_temp",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            }
    
        },
    171:{
        2:{
            "name":"burner1_temp_setpoint",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":1,
            "lower_tolarence":1,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            },
        4:{
            "name":"burner2_temp_setpoint",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":1,
            "lower_tolarence":1,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            },
        6:{
            "name":"burner3_temp_setpoint",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":1,
            "lower_tolarence":1,
            "current_value":0.0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            },
        28:{
            "name":"burner4_temp_setpoint",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":1,
            "lower_tolarence":1,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            }
        },    
    271:{
        24:{
            "name":"burner2_temp",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0.0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            }
    
        },
    272:{
        24:{
            "name":"burner3_temp",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            }
    
        },
    273:{
        24:{
            "name":"burner4_temp",
            "type":"db_int",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            
            }
    
        },
    51:{
        20:{
            "name":"length_setpoint",
            "type":"db_dint",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":10,
            "lower_tolarence":10,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            }
        },
    399:{
        16:{
            "name":"temp_after_dryer1",
            "type":"db_real",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            },
        20:{
            "name":"temp_after_dryer2",
            "type":"db_real",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            },
        24:{
            "name":"temp_after_dryer3",
            "type":"db_real",
            "storing_data_type":"integer",
            "default":"0",
            "precision":0,
            "upper_tolarence":5,
            "lower_tolarence":5,
            "current_value":0,
            "condition_to_log_value":[237,0],
            "condition_to_log":">",
            "condition_to_log_compare":0,
            "function_on_changing":"empty_func"
            }
        }
    
}

cycle_id=0  
#Siemens datatypes
datatypes={"db_real":4,"db_int":2,"db_dint":4,"word":2,"int":2}

cursor = mydb.cursor()
cursor.execute("SELECT cycle_id FROM "+table+" ORDER BY cycle_id DESC LIMIT 0,1")
record = cursor.fetchone()
if record is not None:
    cycle_id=int(record[0])
else:
    cycle_id=1

def asc_cycle_id(new_value):
    global dbs,cycle_id
    if (new_value==0 and dbs[237][4]["current_value"]>0):
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
    global mydb,table
    mycursor=mydb.cursor()
    if storing_data_type=="integer":
        data_value=int(data_value)
    sql="INSERT INTO "+table+" (cycle_id,"+data_name+") VALUES (%s,%s)"
    val=(cycle_id,data_value)
    mycursor.execute(sql,val)
    mydb.commit()
    print(cycle_id,data_name,data_value)



while True: 
    while client.get_connected() == False:
        try:
            client.connect(plc_ip,plc_rack,plc_slot)
            
        except Snap7Exception as e:
            time.sleep(10)
            continue
        
        
    for db in dbs:
        for data in dbs[db]:
            print(dbs[db][data]["name"])
            if db>0:
                get_curr_data=client.db_read(db,data,datatypes[dbs[db][data]["type"]])
            elif db==0:
                get_curr_data=client.read_area(area,0, data, datatypes[dbs[db][data]["type"]])
            
            if dbs[db][data]["type"]=="db_real":
                current_value=round(snap7.util.get_real(get_curr_data,0),dbs[db][data]["precision"])
            elif dbs[db][data]["type"]=="db_int":
                current_value=round(snap7.util.get_int(get_curr_data,0),dbs[db][data]["precision"])
            elif dbs[db][data]["type"]=="db_dint":
                current_value=round(get_dint(get_curr_data,0),dbs[db][data]["precision"])
            elif dbs[db][data]["type"]=="int":
                current_value=round(struct.unpack('!H', get_curr_data)[0]*dbs[db][data]["rescale"],dbs[db][data]["precision"])
                
            if current_value!=dbs[db][data]["current_value"]:
                #diff=abs(dbs[db][data]["current_value"]-current_value)
                print("---------------------")
                print(dbs[db][data]["name"],"actual:",current_value)
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
                    globals()[dbs[db][data]["function_on_changing"]](current_value)
                    send_data(cycle_id,dbs[db][data]["name"],current_value,dbs[db][data]["storing_data_type"])
                    dbs[db][data]["current_value"]=current_value
                
    time.sleep(sample_time)
