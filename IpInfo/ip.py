import pycurl
import json
import time
import csv

from utils import files_handling


def get_info(ip_address):

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
        print(f"\tCountry: {data['country']}")
        print(f"\tAS name: {data['as_name']}")
        

        # append to data_to_store list all the available retrieved info
        data_to_store = []
        keys = ['asn', 'as_name', 'as_domain', 'country_code', 'country', 'continent_code', 'continent']
                
        for key in keys:
            if key in response:
                data_to_store.append(data[key])
            else:
                print(f"\t\t{key} is not available")
                data_to_store.append(None)

        print("#" * 50)
        
        return data_to_store
    
    except Exception as e:
        print(e)




def findIpInfo(zmap_dataset_path):

    try:

        # OUTPUT FILE: IP addresses + IP info
        ip_info_file = files_handling.new_ip_test(zmap_dataset_path)

        # INPUT FILE: IP addresses
        zmap_dataset = open(files_handling.path_dict_to_str(zmap_dataset_path), 'r', newline='')
        ip_list = csv.reader(zmap_dataset)

        for row in ip_list:

            # Retrieve ip information
            ip_info = get_info(row[0])
            # Store ip info data
            row.extend(ip_info)
                    
            files_handling.store_data(row, files_handling.path_dict_to_str(ip_info_file))


    except Exception as e:
        print(e)
    

    return None