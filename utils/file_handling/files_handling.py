import datetime
import os
import csv


def file_selection(levels, path):

    for menu_level in levels:

        #   List of all the available [zmap outputs/experiments/...] alphabetically sorted
        options = os.listdir(path)    # Returns a <list> object
        options.sort()                # Sort the list alphabetically

        if menu_level == 'partition':
            path_to_check = path.replace("IpScan", "DataRefinement")
            path_to_check += f"/{datetime.date.today()}"

            try:
                done_csv = os.listdir(path_to_check)
            except:
                done_csv = []

        while(True):
            #   Print all the datasets together with a zero based index
            print(f"Here's the list of available {menu_level}s to work with:\n")

            for option_id in range(len(options)):
                row = f"\t{option_id:2d}. {options[option_id]}"
                if menu_level == 'partition':
                    if options[option_id] in done_csv:
                        row += "\t[Done]"
                    else:
                        row += "\t[To Do]"
                print(row)

            #   User selection
            print(f"\nSelect the {menu_level} index: ['e' to main menu] ")
            try:
                choice = input()
                if (choice == 'e'):
                    print("-" * 100)
                    return None # -> to MAIN MENU
                
                choice_id = int(choice)

                print("-" * 100)

            except Exception as e:
                print("\tINPUT ERROR: Invalid input")    # Invalid user input
                print(f"\t\t {e}")
                print("-" * 100)
                continue

            if (choice_id not in range(len(options))):
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                print("-" * 100)
                continue

            path += f"/{options[choice_id]}"            
            break

    return path


def store_data(data, file):

    csv_file = open(file, "a+", newline = '')

    with csv_file:
        write = csv.writer(csv_file)
        write.writerow(data)

    csv_file.close()



def allPartitionsDone(coap_path):

    partition_files = os.listdir(coap_path)

    # get total number of partitions from partition filename ex. 1_5.csv
    # first_partition file
    first_part = partition_files[0]
    # partition name
    part_name = first_part.split('.')[0] # get rid of extension
    # number of partitions
    partitions_num = int(part_name.split('_')[1]) # focus on the second number 1_5 = 1of5 -> take 5 

    if partitions_num == len(partition_files):
        return True
    else:
        return False



def new_coap_test(zmap_filepath):

    # ex. IpScan/results_partitioned/02_output.csv/exp_0/4_4.csv
    zmap_filepath_levels = zmap_filepath.split('/')

    # FILE PATH
    # file_path in the DataRefinement scope
    file_path = f"DataRefinement/{zmap_filepath_levels[1]}/{zmap_filepath_levels[2]}/{zmap_filepath_levels[3]}/{datetime.date.today()}"
    
    # if directories do not exit -> create them
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # FILE NAME
    # file path + name
    file_name = f"{file_path}/{zmap_filepath_levels[4]}"

    # create empty file
    with open(file_name, 'w'):
        pass # do nothing
    
    # return file_name
    return file_name



def new_ip_test(coap_filepath):

    # ex. DataRefinement/results_partitioned/02_output.csv/exp_0/2025-08-18
    file_path = coap_filepath.replace("_partitioned", "")

    if not os.path.exists(file_path):
        os.makedirs(file_path)
    
    file_name = f"{file_path}/{datetime.datetime.now()}.csv"

    new_file = open(file_name, "w", newline='')

    # header definition
    writer = csv.writer(new_file)

    writer.writerow(["IP address", "isCoAP", "Code", "Message Type", "Payload", "Payload Size (bytes)",         # from the Resource Discovery
                     "asn", "as_name", "as_domain", "country_code", "country", "continent_code", "continent"])  # from the IpInfo API

    new_file.close()

    return file_name