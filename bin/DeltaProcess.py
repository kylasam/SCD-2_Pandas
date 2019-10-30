import psycopg2
from configparser import ConfigParser
from collections import namedtuple
import pandas as pd
import numpy as np
import sys,time
from datetime import datetime
import logging
import matplotlib.pyplot as plt


CURRENT_DATE=datetime.now().strftime('%Y%m%d')
LOG_FILE_NAME=datetime.now().strftime('../log/delta_processing_%Y%m%d.log')
pd.set_option('display.max_columns', None)
logging.basicConfig(filename=LOG_FILE_NAME,
                    format='%(levelname)s::%(asctime)s.%(msecs)03d  From Module = ":%(funcName)s:" Message=> %(message)s.',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def get_config_details(config_path):
    config=ConfigParser()
    config.read(config_path)
    conf_evaluation={'host' : config.get('database','host'),
          'user' : config.get('database','user'),
          'password' : config.get('database','password'),
          'database' : config.get('database','database'),
          'Key_column' : config.get('database','Key_column'),
          'Delta_data_table_source_columns': config.get ('database', 'Delta_data_table_source_columns'),
          'plot_path': config.get ('workspaces', 'plot_path'),
          'Master_table_columns': config.get ('database', 'Master_table_columns'),
          'Online_order_table' : config.get ('database', 'Online_order_table'),
          'DATA_DIRECTORY': config.get ('workspaces', 'DATA_DIRECTORY'),
          'CURRENT_DAY_DIRECTORY' : config.get('workspaces','CURRENT_DAY_DIRECTORY'),
          'Delta_data_table' : config.get('database','Delta_data_table'),
          'Master_data_table_source_columns' : config.get('database','Master_data_table_source_columns'),
          'report_file' : config.get ('workspaces', 'report_file')
          }
    conf_contents = namedtuple("Config", conf_evaluation.keys ()) (*conf_evaluation.values ())
    return conf_contents

def db_connect_postgresql(hostname,username,database,password):
    logging.info('Connecting to the PostgreSQL database...')
    try:
        conn = psycopg2.connect (host=hostname, database=database, user=username, password=password)
        cur = conn.cursor ()
        logging.info ("Postgresql Connection established Successfully...")
        return cur,conn
    except:
        logging.error("Postgresql Connection FAILED!!!!")
        return False

def CalculateDeltaForTheDay(data_directory,currentday):
    import os
    import pandas as pd
    from datetime import datetime, timedelta
    PREVIUS_DAY=datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    PREVIOUS_DAY_PATTERN="N_MasterFile_"+PREVIUS_DAY+".txt"
    CURRENT_DAY_PATTERN = "O_MasterFile_" + CURRENT_DATE + ".txt"
    logging.info ("Started processing the data with PREVIUS_DAY day " + PREVIUS_DAY +" and current date is " + CURRENT_DATE)

    PREVIOUS_DAY_FILE=os.path.join(data_directory,PREVIUS_DAY,PREVIOUS_DAY_PATTERN)
    CURRENT_DAY_FILE=os.path.join(data_directory,CURRENT_DATE,CURRENT_DAY_PATTERN)
    logging.info("Previous day file consumed from :: " + PREVIOUS_DAY_FILE)
    logging.info("Current day file consumed from :: " + CURRENT_DAY_FILE)
    CURR_DAY=pd.read_csv(CURRENT_DAY_FILE,delimiter='|',header=0,usecols=range(4),names=["C_sor_id", "C_sor_cust_id", "C_CustomerName", "C_PhoneId"],dtype={"C_sor_id" : int, "C_sor_cust_id" : int, "C_CustomerName" : str, "C_PhoneId" : str})
    PREV_DAY=pd.read_csv(PREVIOUS_DAY_FILE, delimiter='|',header=0,usecols=range(4),names=["P_sor_id", "P_sor_cust_id", "P_CustomerName", "P_PhoneId"],dtype={"P_sor_id" : int, "P_sor_cust_id" : int, "P_CustomerName" : str, "P_PhoneId" : str})
    CURR_DAY=CURR_DAY.astype(str)
    PREV_DAY =PREV_DAY.astype(str)
    if CURR_DAY.equals(PREV_DAY):
        logging.error ("Duplicate file Received for the day!! Halting the process with exit code 101....")
        exit(101)
    logging.info("No duplicate file found. Started consuming the records from files")
    CURR_DAY['2PK']=CURR_DAY['C_sor_id'].astype(str) + CURR_DAY['C_sor_cust_id'].astype(str)
    CURR_DAY['C_REST_OF_COLUMNS']=CURR_DAY['C_CustomerName'].astype(str) + CURR_DAY['C_PhoneId'].astype(str)
    PREV_DAY['2PK']=PREV_DAY['P_sor_id'].astype(str) + PREV_DAY['P_sor_cust_id'].astype(str)
    PREV_DAY['P_REST_OF_COLUMNS'] = PREV_DAY['P_CustomerName'].astype (str) + PREV_DAY['P_PhoneId'].astype (str)
    logging.info("Creation of 2PK key and concat of rest of records are done successfully")
    JOIN_DF=pd.merge(PREV_DAY,CURR_DAY,indicator='Existence',on='2PK',how='outer')
    logging.info ("Performing OUTER join on CURRENT & PREVIOUS DAY files.")

    #Find the Delete Records for the day
    DELETE_RECORDS=JOIN_DF.query('Existence == "left_only"')
    DELETE_RECORDS=DELETE_RECORDS[['P_sor_id','P_sor_cust_id','P_CustomerName','P_PhoneId']].rename(columns={'P_sor_id': 'sor_id', 'P_sor_cust_id': 'sor_cust_id','P_CustomerName': 'CustomerName', 'P_PhoneId': 'PhoneId'})
    logging.info ("Total no.of DELETE records for the day :: " + str(DELETE_RECORDS.shape[0]))
    D_RECORDS=DELETE_RECORDS
    D_RECORDS['CHANGE_INDICATOR']='D'

    #Find the Insert Record only
    INSERT_RECORDS=JOIN_DF.query('Existence == "right_only"')
    INSERT_RECORDS=INSERT_RECORDS[['C_sor_id','C_sor_cust_id','C_CustomerName','C_PhoneId']].rename(columns={'C_sor_id': 'sor_id', 'C_sor_cust_id': 'sor_cust_id','C_CustomerName': 'CustomerName', 'C_PhoneId': 'PhoneId'})
    I_RECORDS=INSERT_RECORDS
    logging.info ("Total no.of INSERT records for the day :: " + str(INSERT_RECORDS.shape[0]))
    I_RECORDS['CHANGE_INDICATOR']='I'


    #Find the Update records
    MATCHING_RECORDS = JOIN_DF.query ('Existence == "both"')
    UPDATE_RECORDS=MATCHING_RECORDS.loc[(MATCHING_RECORDS['P_REST_OF_COLUMNS']!=MATCHING_RECORDS['C_REST_OF_COLUMNS'])]
    UPDATE_RECORDS=UPDATE_RECORDS[['C_sor_id','C_sor_cust_id','C_CustomerName','C_PhoneId']].rename(columns={'C_sor_id': 'sor_id', 'C_sor_cust_id': 'sor_cust_id','C_CustomerName': 'CustomerName', 'C_PhoneId': 'PhoneId'})
    logging.info ("Total no.of UPDATE records for the day :: " + str(UPDATE_RECORDS.shape[0]))
    U_RECORDS=UPDATE_RECORDS
    U_RECORDS['CHANGE_INDICATOR']='U'

    #No CHange Records
    NO_CHAGE_RECORDS = MATCHING_RECORDS.loc[(MATCHING_RECORDS['P_REST_OF_COLUMNS'] == MATCHING_RECORDS['C_REST_OF_COLUMNS'])]
    NO_CHAGE_RECORDS=NO_CHAGE_RECORDS[['C_sor_id','C_sor_cust_id','C_CustomerName','C_PhoneId']].rename(columns={'C_sor_id': 'sor_id', 'C_sor_cust_id': 'sor_cust_id','C_CustomerName': 'CustomerName', 'C_PhoneId': 'PhoneId'})
    logging.info ("Total no.of NO CHANGE records for the day :: " + str(NO_CHAGE_RECORDS.shape[0]))
    N_RECORDS=NO_CHAGE_RECORDS
    N_RECORDS['CHANGE_INDICATOR']='N'

    #Create Master File for the day!
    MASTER_FRAMES=[INSERT_RECORDS,UPDATE_RECORDS,NO_CHAGE_RECORDS]
    DELTA_FRAMES=[I_RECORDS,U_RECORDS,D_RECORDS]

    CURRENT_DAY_MASTER_PATTERN ="N_MasterFile_" + CURRENT_DATE + ".txt"
    CURRENT_DAY_DELTA_PATTERN = "DeltaFile_" + CURRENT_DATE + ".txt"

    logging.info ("Current day MASTER file Name :: " + CURRENT_DAY_MASTER_PATTERN)
    logging.info ("Current day DELTA file Name :: " + CURRENT_DAY_DELTA_PATTERN)
    DELTA_FILE=pd.concat(DELTA_FRAMES)
    MASTER_FILE = pd.concat(MASTER_FRAMES)
    MASTER_FILE=MASTER_FILE[['sor_id','sor_cust_id','CustomerName','PhoneId']]
    try:
        logging.info("Trying block to dump the Master and delta file into the disk..")
        MASTER_FILE.to_csv ((os.path.join (data_directory, CURRENT_DATE, CURRENT_DAY_MASTER_PATTERN)), sep='|', index=False)
        DELTA_FILE.to_csv((os.path.join(data_directory,CURRENT_DATE,CURRENT_DAY_DELTA_PATTERN)),sep='|',index=False)
        logging.info("Current day Master file Details :: " + (os.path.join (data_directory, CURRENT_DATE, CURRENT_DAY_MASTER_PATTERN)))
        logging.info("Current dat delta file details :: " + (os.path.join (data_directory, CURRENT_DATE, CURRENT_DAY_DELTA_PATTERN)))
    except Exception as err:
        logging.error(err)

    return ((os.path.join(data_directory,CURRENT_DATE,CURRENT_DAY_DELTA_PATTERN)))

def Visualization(filepath,pltsave):
    import matplotlib.pyplot as plt
    import numpy as np
    import os

    DeltaFile=pd.read_csv(filepath,sep="|")
    DeltaFile['sor_description']=np.where(DeltaFile['sor_id']==2,"OnlineCustmers","OfflineCustomers")
    DeltaFile.groupby (['CHANGE_INDICATOR', 'sor_description']).size ().unstack ().plot (kind='bar', stacked=True)
    plt.xlabel("Insert/Update/Delete Customers per day")
    plt.ylabel ("No.of Customers")
    plt.suptitle("Customer Trend Analysis for the day!!",fontsize=11)
    plt.savefig(pltsave + 'CustomerTrendAnalysis_' + CURRENT_DATE + ".png",bbox_inches = "tight")
    plt.show()
    logging.info ("Plot has been drewn successfully in :: " + (os.path.join (pltsave, 'CustomerTrendAnalysis_' + CURRENT_DATE + ".png")))





def main():
    # Get configuration values from external file
    conf = "..\config.ini"
    config = get_config_details(conf)
    logging.info("Started the Delta process....")

    DeltaProcess=CalculateDeltaForTheDay(config.DATA_DIRECTORY,config.report_file)

    CreateStatOnDeltaProcess=Visualization(DeltaProcess,config.plot_path)
    #Evaluate the Connection details for Postgresql
    #initialze_db,initialze_conn=db_connect_postgresql(config.host,config.user,config.database,config.password)


if __name__ == '__main__':
 RETVAL = main()
 sys.exit(RETVAL)
