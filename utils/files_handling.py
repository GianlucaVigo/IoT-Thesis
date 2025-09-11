import datetime
import os
import csv
import pandas as pd


def level_selection(level, path):

    string_path = path_dict_to_str(path)

    try:
        #   List of all the available [zmap outputs/experiments/...] alphabetically sorted
        options = os.listdir(string_path)       # Returns a <list> object
        options.sort()                          # Sort the list alphabetically

    except Exception as e:
        print(f"\t{e}")
        print("\n\t[Redirection to main menu ...]\n")
        return None # -> to MAIN MENU

    match level:
        case "dataset":
            metadata_info = get_datasets_info()
        case "experiment":
            dataset = path['dataset']
            metadata_info = get_exps_info(dataset)
        case "partition":
            dataset = path['dataset']
            experiment = path['experiment']
            metadata_info = get_parts_info(dataset, experiment)
        case "date":
            dataset = path['dataset']
            experiment = path['experiment']
            metadata_info = get_dates_info(dataset, experiment)

    while(True):
        print('?' * 40, f"{level.capitalize()} Selection", '?' * 40)

        #   Print all together with a zero based index
        print(f"Here's the list of available {level}s to work with:\n")

        # printing header
        print("\tindex".ljust(10), end="")
        for key in metadata_info[0].keys():
            print(key.ljust(20), end="")
        print()

        # printing list of options
        for option_id in range(len(options)):
            print(f"\t{option_id}".ljust(10), end="")
            try:
                for value in metadata_info[option_id].values():
                    print(str(value).ljust(20), end="")
            except Exception as e:
                print(f"\t[ERROR] No metadata have been collected yet\n\t\t{e}")
            print()


        #   User selects index
        print(f"\nSelect the {level} index: ['e' to main menu] ")
        try:
            choice = input()
            print()

            if (choice == 'e'):
                print("\n\t[Redirection to main menu ...]\n")
                return None # -> to MAIN MENU
                
            choice_id = int(choice)

        except Exception as e:
            print("\tINPUT ERROR: Invalid input\n")    # Invalid user input
            print(f"\t\t {e}")
            continue

        if (choice_id not in range(len(options))):
            print("\tINPUT ERROR: Invalid option -> Out of range!\n")    # Invalid user choice
            continue

        break

    return options[choice_id]



def path_dict_to_str(dict_path):

    string_path = ""

    for value in dict_path.values():
        string_path += f"{value}/"

    return string_path[:-1]



def store_data(data, file):

    with open(file, "a", newline = '') as csv_file:
        write = csv.writer(csv_file)
        write.writerow(data)



def all_partitions_done(path):

    # csv containing info about experiments associated to a dataset
    csv_metadata = "utils/logs/experiments.csv"
    # csv -> pandas dataframe
    experiment_row = pd.read_csv(csv_metadata)
    # find the rows associated to the specified dataset and experiment -> partitions
    experiment_row = experiment_row.loc[((experiment_row['exp_name'] == path['experiment']) &
                                        (experiment_row['dataset_name'] == path['dataset']))]
    
    n_partitions = experiment_row['n_partitions'].iloc[0]

    # csv containing log info about tests 
    match path['phase']:
        case 'Discovery':
            tests_log = "utils/logs/discovery_checks.csv"
        case 'GetResource':
            tests_log = "utils/logs/get_checks.csv"
        case 'Observe':
            tests_log = "utils/logs/observe_checks.csv"

    
    # csv -> pandas dataframe
    tests_log_df = pd.read_csv(tests_log)
    # find the rows associated to the specified dataset and experiment -> partitions
    tests_log_df = tests_log_df.loc[((tests_log_df['exp_name'] == path['experiment']) &
                                    (tests_log_df['dataset_name'] == path['dataset']) &
                                    (tests_log_df['date'] == path['date']))]

    n_partitions_done = tests_log_df.shape[0]

    if n_partitions == n_partitions_done:
        return True
    else:
        return False



def new_discovery_test(partition_path):

    # Before: Partitioning/csv/02_output.csv/exp_0/4_4.csv
    coap_test_path = {
        'phase' : 'Discovery',
        'folder': 'csv',
        'dataset': partition_path['dataset'],
        'experiment': partition_path['experiment'],
        'date': datetime.date.today()
    }
    # After: Discovery/csv/02_output.csv/exp_0/2025-08-23

    coap_test_path_str = path_dict_to_str(coap_test_path)
    
    # if directories do not exit -> create them
    if not os.path.exists(coap_test_path_str):
        os.makedirs(coap_test_path_str)

    # Before: Discovery/csv/02_output.csv/exp_0/2025-08-23
    coap_test_path.update({'partition': partition_path['partition']})
    #  After: Discovery/csv/02_output.csv/exp_0/2025-08-23/4_4.csv
    
    coap_test_path_str = path_dict_to_str(coap_test_path)

    # create empty file
    with open(coap_test_path_str, 'w') as coap_partition_test:
        # HEADER
        writer = csv.writer(coap_partition_test)
        writer.writerow(["IP address", "asn", "as_name", "as_domain", "country_code", "country", "continent_code", "continent",     # from the IpInfo API
                        "isCoAP", "Code", "Message Type", "Payload", "Payload Size (bytes)"])                                       # from Resource Discovery
        coap_partition_test.close()
    
    # return coap test path
    return coap_test_path


def new_gets_test(partition_path):

    # Before: Discovery/csv/02_output.csv/exp_0/2025-08-23/4_4.csv
    get_test_path = {
        'phase' : 'GetResource',
        'folder': 'csv',
        'dataset': partition_path['dataset'],
        'experiment': partition_path['experiment'],
        'date': partition_path['date']
    }
    # After: GetResource/csv/02_output.csv/exp_0/2025-08-23

    get_test_path_str = path_dict_to_str(get_test_path)
    
    # if directories do not exit -> create them
    if not os.path.exists(get_test_path_str):
        os.makedirs(get_test_path_str)

    # Before: GetResource/csv/02_output.csv/exp_0/2025-08-23
    get_test_path.update({'partition': partition_path['partition']})
    #  After: GetResource/csv/02_output.csv/exp_0/2025-08-23/4_4.csv
    
    get_test_path_str = path_dict_to_str(get_test_path)

    # create empty file
    with open(get_test_path_str, 'w') as get_partition_test:
        # HEADER
        writer = csv.writer(get_partition_test)
        writer.writerow(["IP address", "as_name", "country", "continent",     # from the IpInfo API
                        "uri", "Code", "Message Type", "Payload", "Payload Size (bytes)"])                                       # from Resource Discovery
        get_partition_test.close()
    
    # return coap test path
    return get_test_path


def new_ip_test(zmap_filepath):

    '''DIRECTORIES ORGANIZATION'''
    ipinfo_path = {
        'phase' : 'IpInfo',
        'folder': 'csv'
    }

    ipinfo_path_str = path_dict_to_str(ipinfo_path)

    if not os.path.exists(ipinfo_path_str):
        os.makedirs(ipinfo_path_str)

    '''NEW FILE DEFINITION'''
    # Before: IpInfo/csv/
    ipinfo_path.update({'dataset': zmap_filepath['dataset']})
    #  After: IpInfo/csv/01_output.csv
    
    ipinfo_path_str = path_dict_to_str(ipinfo_path)

    '''CREATE EMPTY FILE'''
    # create empty file
    with open(ipinfo_path_str, 'w'):
        pass # do nothing

    return ipinfo_path



# return a dictionary of additional info associated to a zmap dataset
def get_datasets_info():

    # csv containing info about zmap dataset
    csv_metadata = "utils/logs/zmap_datasets.csv"

    # csv -> pandas dataframe
    dataset_rows = pd.read_csv(csv_metadata)

    res = []

    for i in range(dataset_rows.shape[0]):
        # extract valuable info: date, number of IPs
        dataset = dataset_rows.iloc[i]['dataset_name']
        date = dataset_rows.iloc[i]['date']
        n_ips = dataset_rows.iloc[i]['n_ips']

        # organize info as dictionary
        info_row = {'dataset': dataset, 'date': date, 'n_ips': n_ips}

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res



# returns a list of dictionaries, one for each experiment performed over a ipinfo dataset
def get_exps_info(dataset_name):

    # csv containing info about experiments performed on datasets
    csv_metadata = "utils/logs/experiments.csv"

    # csv -> pandas dataframe
    experiments_rows = pd.read_csv(csv_metadata)

    # find the row associated to the specified dataset name -> experiments
    experiments_rows = experiments_rows.loc[experiments_rows['dataset_name'] == dataset_name]

    res = []

    for i in range(experiments_rows.shape[0]):

        # extract valuable info: exp_name, date, n_partitions
        exp_name = experiments_rows.iloc[i]['exp_name']
        date = experiments_rows.iloc[i]['date']
        n_partitions = int(experiments_rows.iloc[i]['n_partitions'])

        # organize info as dictionary
        info_row = {'exp_name': exp_name, 'date': date, 'n_partitions': n_partitions}

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res



# returns a list of dictionaries, one for each partition related to an experiment over a dataset
def get_parts_info(zmap, exp):

    # csv containing info about partitions of an experiment
    csv_metadata = "utils/logs/partitions.csv"

    # csv -> pandas dataframe
    partition_rows = pd.read_csv(csv_metadata)

    # find the rows associated to the specified dataset and experiment -> partitions
    partition_rows = partition_rows.loc[((partition_rows['exp_name'] == exp) &
                                        (partition_rows['dataset_name'] == zmap))]

    res = []

    for i in range(partition_rows.shape[0]):

        # extract valuable info: partition id, partition size
        part_id = partition_rows.iloc[i]['partition_id']
        part_size = partition_rows.iloc[i]['partition_size']
        part_done = is_part_done(zmap, exp, str(datetime.date.today()), part_id)
        
        # organize info as dictionary
        info_row = {'partition_id': part_id,
                    'partition_size': part_size,
                    'discovery_test': part_done[0],
                    'get_test': part_done[1],
                    'observe_test': part_done[2]}

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res



# returns a metadata string needed to understand whether, during the current day, 
# the coap test over a specified partition has been already performed or not
def is_part_done(zmap, exp, date, part_id):

    # csv containing info about partitions of an experiment
    csvs_metadata = ["utils/logs/discovery_checks.csv",
                     "utils/logs/get_checks.csv",
                     "utils/logs/observe_checks.csv"]
    
    metadata_res = []

    for csv in csvs_metadata:

        # csv -> pandas dataframe
        partition_rows = pd.read_csv(csv)

        # find the rows associated to the specified dataset, experiment, date and partition id
        partition_rows = partition_rows.loc[((partition_rows['exp_name'] == exp) &
                                            (partition_rows['dataset_name'] == zmap) &
                                            (partition_rows['date'] == str(date)) &
                                            (partition_rows['partition_id'] == int(part_id))
                                            )]

        # return the partition state string
        if partition_rows.empty:
            metadata_res.append("[To Do]")
        else:
            metadata_res.append("[Done]")
    
    return metadata_res
    


# returns a metadata string needed to understand whether a date has all the coap tests done
def get_dates_info(zmap_dataset, experiment):

    # csv containing info about experiments associated to a dataset
    csv_metadata = "utils/logs/experiments.csv"
    # csv -> pandas dataframe
    experiment_row = pd.read_csv(csv_metadata)
    # find the rows associated to the specified dataset and experiment -> partitions
    experiment_row = experiment_row.loc[((experiment_row['exp_name'] == experiment) &
                                        (experiment_row['dataset_name'] == zmap_dataset))]
    
    n_partitions = experiment_row['n_partitions'].iloc[0]


    # csv containing log info about discovery tests 
    coap_tests_log = "utils/logs/discovery_checks.csv"
    # csv -> pandas dataframe
    coap_tests_log_rows = pd.read_csv(coap_tests_log)
    # find the rows associated to the specified dataset and experiment -> partitions
    coap_tests_log_rows = coap_tests_log_rows.loc[((coap_tests_log_rows['exp_name'] == experiment) &
                                                (coap_tests_log_rows['dataset_name'] == zmap_dataset))]
    
    n_part_per_date = coap_tests_log_rows['date'].value_counts().sort_index()

    res = []

    for date, count in n_part_per_date.items():

        if count == n_partitions:
            date_done = "[True]"
        else:
            date_done = "[False]"
        
        # organize info as dictionary
        info_row = {'date': date, 'all_partitions_done': date_done}

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res