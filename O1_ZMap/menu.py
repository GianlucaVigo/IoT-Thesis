import csv
import os
import asyncio
import pandas as pd
import pycurl
import json
import time
from datetime import date as dt

from aiocoap import *
from utils import files_handling


# perform the resource discovery operation -> GET (/.well-known/core)
async def discovery(ip_address):

    # client context creation
    protocol = await Context.create_client_context()
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683/.well-known/core"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)

    try:
        # send the request and obtained the response
        response = await protocol.request(request).response

    except Exception as e:
        print(f"\t{e}")
        return e # error

    else:
        return response # success


async def decode_payload(df_zmap):

    new_data_list = []

    for index, row in df_zmap.iterrows():

        print('£' * 50)

        # getting binary data 
        binary_data = row['data']
        # interpreting binary data through aiocoap library
        msg = Message.decode(bytes.fromhex(binary_data))
        # discovery payload = list of uris + their metadata
        decoded_payload = msg.payload.decode('utf-8')
        print(f"\tRaw Payload Decoded: {decoded_payload}")

        # is BLOCK 2 option enabled?
        block2 = msg.opt.block2

        # T: Block based communication -> ZMap collected only first block ingnoring the remaining ones
        # -> perform a new discovery to get a complete result
        if block2:
            print(f"\nBlock2 detected!")
            print(f"\tBlock number: {block2.block_number}")
            print(f"\tMore blocks flag: {block2.more}")
            print(f"\tBlock size: {block2.size} bytes\n")


            # perform discovery
            print("\tPerforming a new discovery ...\n")
            complete_discovery = await discovery(row[0])

            print(f"\t\tComplete Payload Decoded: {complete_discovery.payload.decode('utf-8')}")

            new_data_list.append(complete_discovery.payload.decode('utf-8'))

        # F: No block based communication -> ZMap result is already complete
        else:
            print("\nNo Block2 option detected. Full payload in single message.")
            new_data_list.append(decoded_payload)

    df_zmap['data'] = new_data_list
    
    return df_zmap



def update_discovery_log():

    # dataset selection
    dataset_to_update = files_handling.level_selection('dataset', None)
    discovery_datasets_path = "O1_ZMap/scan_results/csv/" + dataset_to_update

    # LIST OF DATES IN THE FILE SYSTEM
    # get all dates associated to selected dataset
    discovery_dates = os.listdir(discovery_datasets_path)
    discovery_dates.sort()

    # path of discovery.csv log
    discovery_log_path = "utils/logs/discovery.csv"

    # LIST OF DATES IN THE LOG
    # list of available dates 
    discovery_dates_log = []

    # open discovery log file
    with open(discovery_log_path, newline='') as discovery_log:

        # interprete the file as a csv
        discovery_log_rows = csv.reader(discovery_log)

        # iterating over discovery log rows
        for row in discovery_log_rows:
                
            # skip header
            if (discovery_log_rows.line_num == 1): 
                continue

            # checks only entries related to the user selected dataset
            # column 1 -> dataset
            if row[1] == dataset_to_update:
                    
                # append the date
                # column 0 -> date
                discovery_dates_log.append(row[0])
    

    # LOG SYNCHRONIZATION
    # check if dates in the filesystem are already in the log or not
    for date_csv in discovery_dates:

        print(";" * 50)

        # cut '.csv' part of the string
        date = date_csv[:-4]

        # date already in the log
        if date in discovery_dates_log:
            print(f"{date} already present in the log")
        # date NOT in the log
        else:
            print(f"{date} NOT present in the log!")
            print(f"\tUpdating the log with new info!")
            # add entry in the log
            files_handling.store_data([date, dataset_to_update], discovery_log_path)

    print()

    return



def remove_duplicates(df_zmap):

    df_zmap.drop_duplicates(subset=['saddr', 'data'], keep='first', inplace=True)

    df_zmap.reset_index(inplace=True, drop=True)

    return df_zmap



def get_info(ip_address):

    print("#" * 75)

    try: 
        # in order to be compliant with the max API rate (= 3 req/s)
        time.sleep(0.5)

        # Create a Curl object
        curl = pycurl.Curl()

        # requested URL -> passing IP address to test + auth token
        url = f"https://api.ipinfo.io/lite/{ip_address}?token=9730afaa10492f"
        # response type -> JSON
        header = ['Accept: application/json']

        # Set the URL to send the GET request
        curl.setopt(curl.URL, url)
        curl.setopt(curl.HTTPHEADER, header)

        # Perform the request - perform_rs
        response = curl.perform_rs()

        # Close the Curl object
        curl.close()

        # interpreting JSON format
        data = json.loads(response)

        # debug
        print(f"Extract info about: {ip_address}" ) # row[0] => ip address
        print(f"\tCountry: {data.get('country')}")
        print(f"\tAS name: {data.get('as_name')}")

        # build one-row DataFrame
        data_to_store = pd.DataFrame([{
            "asn": data.get("asn"),
            "as_name": data.get("as_name"),
            "as_domain": data.get("as_domain"),
            "country_code": data.get("country_code"),
            "country": data.get("country"),
            "continent_code": data.get("continent_code"),
            "continent": data.get("continent"),
        }])

        return data_to_store
    
    except Exception as e:
        print(f"get_info: {e}")
        return pd.DataFrame()


def extract_ip_info(ip_list_df):
    ip_info_df = pd.DataFrame()

    for index, row in ip_list_df.iterrows():
        info = get_info(row['saddr'])
        ip_info_df = pd.concat([ip_info_df, info], ignore_index=True)
        
    # merge original IPs with retrieved info
    ip_info_df = pd.concat([ip_list_df.reset_index(drop=True), ip_info_df], axis=1)

    return ip_info_df


######################################################################################################

def new_master_dataset_check():

    print("New Master Datasets utility ...")
    
    # zmap datasets locations
    zmap_datasets_path = {'phase': 'O1_ZMap/scan_results', 'folder': 'csv'}
    # available datesets
    available_datasets = os.listdir(files_handling.path_dict_to_str(zmap_datasets_path))
    available_datasets.sort()


    # datasets log path
    zmap_datasets_log_path = "utils/logs/zmap_datasets.csv"
    zmap_discovery_log_path = "utils/logs/discovery.csv"
    # logged datasets
    logged_datasets = []

    zmap_datasets_log = pd.read_csv(zmap_datasets_log_path)

    for index, row in zmap_datasets_log.iterrows():
        logged_datasets.append(row['dataset'])

    print(f"Available Datasets in FS: {available_datasets}")
    print(f"Logged Datasets: {logged_datasets}")

    for dataset in available_datasets:
        # NEW DATASET to be logged
        if dataset not in logged_datasets:

            print(f"{dataset} not logged yet!")

            zmap_datasets_path.update({'dataset': dataset})

            print(zmap_datasets_path)

            # ----------- date -----------
            master_date = os.listdir(files_handling.path_dict_to_str(zmap_datasets_path))
            master_date = master_date[0][:-4]

            print(f"Current Date: {dt.today()} | Master Date: {master_date}")

            if str(dt.today()) != master_date:
                continue

            zmap_datasets_path.update({'date': master_date})

            # ----------- cleaned-zmap-dataset -----------
            clean_zmap_datasets_path = {'phase': zmap_datasets_path['phase'],
                                        'folder': 'cleaned',
                                        'dataset': zmap_datasets_path['dataset']}
            # if directories do not exist -> create them
            if not os.path.exists(files_handling.path_dict_to_str(clean_zmap_datasets_path)):
                os.makedirs(files_handling.path_dict_to_str(clean_zmap_datasets_path))
            
            clean_zmap_datasets_path.update({'date': zmap_datasets_path['date']})

            # ----------- raw-master-dataset -----------
            master_df = pd.read_csv(files_handling.path_dict_to_str(zmap_datasets_path) + '.csv')

            # ----------- remove-duplicates -----------
            master_df = remove_duplicates(master_df)

            # ----------- decode-zmap-payload -----------
            master_df = asyncio.run(decode_payload(master_df))

            # ----------- extract-ip-info -----------
            ip_info_path = {'phase': zmap_datasets_path['phase'],
                            'folder': 'ip_info',
                            'dataset': zmap_datasets_path['dataset']}
            ip_info_df = extract_ip_info(master_df[['saddr']])

            ip_info_df.to_csv(files_handling.path_dict_to_str(ip_info_path) + '.csv', index=False)

            # ----------- n-ips -----------
            n_ips = master_df.shape[0]

            # ----------- store-master-dataframe -----------
            master_df.to_csv(files_handling.path_dict_to_str(clean_zmap_datasets_path) + '.csv', index=False)

            # ----------- ZMAP_DATASETS-log -----------
            files_handling.store_data([zmap_datasets_path['dataset'], zmap_datasets_path['date'], n_ips], zmap_datasets_log_path)
            # ----------- ZMAP_DISCOVERY-log -----------
            files_handling.store_data([zmap_datasets_path['date'], zmap_datasets_path['dataset']], zmap_discovery_log_path)

            zmap_datasets_path.pop('date')
        else:
            print(f"{dataset} is already logged")
        
    return 



def new_dataset_dates_check():

    # ----------- DISCOVERIES LOGGED -----------
    # discovery log path
    zmap_discovery_log_path = "utils/logs/discovery.csv"
    # logged discovery  
    zmap_discovery_log_df = pd.read_csv(zmap_discovery_log_path)


    # ----------- DISCOVERIES in FILE SYSTEM -----------
    # zmap dates locations
    zmap_dates_path = {'phase': 'O1_ZMap/scan_results', 'folder': 'csv'}
    # available datesets
    available_datasets = os.listdir(files_handling.path_dict_to_str(zmap_dates_path))
    available_datasets.sort()

    for dataset in available_datasets:
        zmap_dates_path.update({'dataset': dataset})

        # available dates
        available_dates = os.listdir(files_handling.path_dict_to_str(zmap_dates_path))
        available_dates.sort()

        for date in available_dates:

            if str(dt.today()) != date[:-4]:
                continue
            
            res_df = zmap_discovery_log_df[(zmap_discovery_log_df['date'] == date[:-4]) & (zmap_discovery_log_df['dataset'] == dataset)]

            # current date of the current dataset has not been inserted in the log
            if res_df.empty: 

                # ----------- date -----------
                zmap_dates_path.update({'date': date[:-4]})

                # ----------- cleaned-zmap-dataset -----------
                clean_zmap_datasets_path = {'phase': zmap_dates_path['phase'],
                                            'folder': 'cleaned',
                                            'dataset': zmap_dates_path['dataset']}
                # if directories do not exist -> create them
                if not os.path.exists(files_handling.path_dict_to_str(clean_zmap_datasets_path)):
                    os.makedirs(files_handling.path_dict_to_str(clean_zmap_datasets_path))
                
                clean_zmap_datasets_path.update({'date': zmap_dates_path['date']})

                # ----------- raw-master-dataset -----------
                discovery_df = pd.read_csv(files_handling.path_dict_to_str(zmap_dates_path) + '.csv')

                # ----------- remove-duplicates -----------
                discovery_df = remove_duplicates(discovery_df)

                # ----------- decode-zmap-payload -----------
                discovery_df = asyncio.run(decode_payload(discovery_df))

                # ----------- store-master-dataframe -----------
                discovery_df.to_csv(files_handling.path_dict_to_str(clean_zmap_datasets_path) + '.csv', index=False)

                # ----------- DISCOVERY-log -----------
                files_handling.store_data([zmap_dates_path['date'], zmap_dates_path['dataset']], zmap_discovery_log_path)

                zmap_dates_path.pop('date')

    return


def zmap_menu():

    new_master_dataset_check()

    new_dataset_dates_check()

    return