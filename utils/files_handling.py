import datetime
import csv
import pandas as pd
import os


def level_selection(level, path):

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
            metadata_info = get_dates_info(dataset)

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
        for option_id in range(len(metadata_info)):
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

        if (choice_id not in range(len(metadata_info))):
            print("\tINPUT ERROR: Invalid option -> Out of range!\n")    # Invalid user choice
            continue

        break

    return metadata_info[choice_id][level]



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
    experiment_row = experiment_row.loc[((experiment_row['experiment'] == path['experiment']) &
                                        (experiment_row['dataset'] == path['dataset']))]
    
    n_partitions = experiment_row['n_partitions'].iloc[0]

    # csv containing log info about tests 
    match path['phase']:
        case 'O5_GetResource':
            tests_log = "utils/logs/get.csv"
        case 'O6_Observe':
            tests_log = "utils/logs/observe.csv"

    
    # csv -> pandas dataframe
    tests_log_df = pd.read_csv(tests_log)
    # find the rows associated to the specified dataset and experiment -> partitions
    tests_log_df = tests_log_df.loc[((tests_log_df['experiment'] == path['experiment']) &
                                    (tests_log_df['dataset'] == path['dataset']) &
                                    (tests_log_df['date'] == path['date']))]

    tests_log_df = tests_log_df.drop_duplicates(keep='first')

    n_partitions_done = tests_log_df['partition'].shape[0]

    if n_partitions == n_partitions_done:
        return True
    else:
        return False



# return a dictionary of additional info associated to a zmap dataset
def get_datasets_info():

    # csv containing info about zmap dataset
    csv_metadata = "utils/logs/zmap_datasets.csv"

    # csv -> pandas dataframe
    dataset_rows = pd.read_csv(csv_metadata)

    res = []

    for i in range(dataset_rows.shape[0]):
        # extract valuable info: date, number of IPs
        dataset = str(dataset_rows.iloc[i]['dataset'])
        date = str(dataset_rows.iloc[i]['date'])
        n_ips = int(dataset_rows.iloc[i]['n_ips'])

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
    experiments_rows = experiments_rows.loc[experiments_rows['dataset'] == dataset_name]

    res = []

    for i in range(experiments_rows.shape[0]):

        # extract valuable info: exp_name, date, n_partitions
        experiment = str(experiments_rows.iloc[i]['experiment'])
        date = str(experiments_rows.iloc[i]['date'])
        n_partitions = int(experiments_rows.iloc[i]['n_partitions'])

        # organize info as dictionary
        info_row = {'experiment': experiment, 'date': date, 'n_partitions': n_partitions}

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
    partition_rows = partition_rows.loc[((partition_rows['experiment'] == exp) &
                                        (partition_rows['dataset'] == zmap))]

    res = []

    for i in range(partition_rows.shape[0]):

        # extract valuable info: partition id, partition size
        part_id = int(partition_rows.iloc[i]['partition'])
        part_discovery_status = is_discovery_done(zmap)
        part_get_status = is_get_done(zmap, exp, str(datetime.date.today()), part_id)
        part_observe_status = is_observe_done(zmap, exp, str(datetime.date.today()), part_id)
        
        # organize info as dictionary
        info_row = {'partition': part_id,
                    'is_discovery_done': part_discovery_status,
                    'is_get_done': part_get_status,
                    'is_observe_done': part_observe_status
                    }

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res



# returns a list of dictionaries
def get_dates_info(dataset):

    # csv containing info about partitions of an experiment
    csv_metadata = "utils/logs/discovery.csv"

    # csv -> pandas dataframe
    date_rows = pd.read_csv(csv_metadata)

    # find the rows associated to the specified dataset and experiment -> partitions
    date_rows = date_rows.loc[(date_rows['dataset'] == dataset)]

    res = []

    for i in range(date_rows.shape[0]):

        # extract valuable info: date
        date = str(date_rows.iloc[i]['date'])
        
        # organize info as dictionary
        info_row = {'date': date}

        # append dictionary to res
        res.append(info_row)

    # return list of dictionaries
    return res



# returns a metadata string needed to understand whether, during the current day, 
# the coap test over a specified partition has been already performed or not
def is_observe_done(zmap, exp, date, part_id):

    csv_metadata = "utils/logs/observe.csv"
    
    metadata_res = []

    # csv -> pandas dataframe
    partition_rows = pd.read_csv(csv_metadata)

    # find the rows associated to the specified dataset, experiment, date and partition id
    partition_rows = partition_rows.loc[((partition_rows['experiment'] == exp) &
                                            (partition_rows['dataset'] == zmap) &
                                            (partition_rows['date'] == str(date)) &
                                            (partition_rows['partition'] == int(part_id))
                                            )]

    # return the partition state string
    if partition_rows.empty:
        metadata_res.append(False)
    else:
        metadata_res.append(True)
    
    return metadata_res



# returns a metadata string needed to understand whether, during the current day, 
# the coap test over a specified partition has been already performed or not
def is_get_done(zmap, exp, date, part_id):

    csv_metadata = "utils/logs/get.csv"
    
    metadata_res = []

    # csv -> pandas dataframe
    partition_rows = pd.read_csv(csv_metadata)

    # find the rows associated to the specified dataset, experiment, date and partition id
    partition_rows = partition_rows.loc[((partition_rows['experiment'] == exp) &
                                            (partition_rows['dataset'] == zmap) &
                                            (partition_rows['date'] == date) &
                                            (partition_rows['partition'] == part_id)
                                            )]

    # return the partition state string
    if partition_rows.empty:
        metadata_res.append(False)
    else:
        metadata_res.append(True)
    
    return metadata_res



# returns a metadata string needed to understand whether, during the current day, 
# the coap test over a specified partition has been already performed or not
def is_discovery_done(zmap):

    csv_metadata = "utils/logs/discovery.csv"
    
    metadata_res = []

    # csv -> pandas dataframe
    partition_rows = pd.read_csv(csv_metadata)

    # find the rows associated to the specified dataset, experiment, date and partition id
    partition_rows = partition_rows.loc[((partition_rows['dataset'] == zmap) &
                                        (partition_rows['date'] == str(datetime.date.today()))
                                        )]

    # return the partition state string
    if partition_rows.empty:
        metadata_res.append(False)
    else:
        metadata_res.append(True)
    
    return metadata_res



def is_discovery_available(dataset):

    available_dates = get_dates_info(dataset)

    for date in available_dates:
        if str(datetime.date.today()) == date['date']:
            return True

    return False



def extract_partitions(partitions_path):
    # date info
    print('%' * 50)
    print(f"Date: {partitions_path['date']}")
    
    if all_partitions_done(partitions_path):
        print("\tAll partitions have been processed ...")

        csvs = os.listdir(path_dict_to_str(partitions_path))
        csvs.sort()

        return csvs

    else:
        print("Some partitions have not been processed yet!")
        return None
    


def read_file_system(base_path):

    file_system = []

    items = os.listdir(base_path)
    items.sort()

    if not items:
        return []

    for item in items:

        to_test = base_path + f"/{item}"

        if os.path.isfile(to_test):
            file_system.append(to_test)
        else:
            file_system.extend(read_file_system(to_test))

    return file_system
