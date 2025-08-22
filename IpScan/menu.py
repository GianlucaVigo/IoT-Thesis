import os
from utils.file_handling import files_handling
import datetime

def zmap_menu():

    # 1) base zmap dataset selection
    source_zmap_path = "IpScan/results"

    #   List of all the available zmap output csv files alphabetically sorted
    datasets = os.listdir(source_zmap_path) # returns a <list> object
    datasets.sort()                         # sort the list alphabetically
    
    while(True):    
        #   Print all the datasets together with a zero based index
        print("Here's the list of available zmap output datasets to work with:\n")
        for dataset_id in range(len(datasets)):

            dataset_name = datasets[dataset_id]

            if dataset_name.find("results") == -1:
                dataset_info = files_handling.get_dataset_info(dataset_name)
                print(f"\t{dataset_id}. {dataset_name} | date: {dataset_info[0]} | nr_ips: {dataset_info[1]}") 

        #   User selection
        print("\nSelect the raw dataset index: ['e' to main menu] ")
        try:
            choice = input()
            print("-" * 100)
            if (choice == 'e'):
                return None # -> to MAIN MENU
            
            dataset_id = int(choice)

        except Exception as e:
            print("\tINPUT ERROR: Invalid input")    # Invalid user input
            print(f"\t\t {e}")
            print("-" * 100)
            continue
        
        if (dataset_id not in range(len(datasets))):
            print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
            print("-" * 100)
            continue

        source_zmap_path += f"/{datasets[dataset_id]}"
        break


    # 2) user defines the number of partitions to be created

    exps = files_handling.get_exp_info(datasets[dataset_id])

    while(True):

        while(True):
            print("Here's the list of experiments performed on the selected dataset: \n")

            for exp in exps:
                print(f"\texp_name: {exp[0]} | date: {exp[1]} | nr_partitions: {exp[2]}")
            
            print("\nDo you want to create a NEW experiment? [Y/n]")

            answer = input()
            match answer:
                case "Y":
                    break
                case "n":
                    print("\t[Redirection to main menu ...]")    # To main menu
                    print("-" * 100)
                    return
                case _:
                    print("\tINPUT ERROR: Invalid input")    # Invalid user choice
                    print("-" * 100)

        print("\nHow many csv partitions do you want to create? MAX: 20 ['e' to main menu]")

        #   upperbound limit over the number of partitions to be created
        n_partitions_upperbound = 20

        try:
            choice = input()
            print("-" * 100)
            if (choice == 'e'):
                return # -> to MAIN MENU
            
            n_partitions = int(choice)

        except Exception as e:
            print("\tINPUT ERROR: Invalid input")    # Invalid user input
            print(f"\t\t {e}")
            print("-" * 100)
            continue

        if (n_partitions > n_partitions_upperbound):
            print("\tINPUT ERROR: Invalid option -> Too many partitions!")    # Invalid user choice
            print("-" * 100)
            continue
        
        if (n_partitions <= 0):
            print("\tINPUT ERROR: Invalid option -> Number of partitions can't be zero or negative!")    # Invalid user choice
            print("-" * 100)
            continue
    
        break


    # 3) directory structure preparation
    #   a. Source Dataset selection
    zmap_partitions_path = "IpScan/results_partitioned"
    zmap_partitions_path += f"/{datasets[dataset_id]}"

    #       if not already present the directory path, create it
    if not os.path.exists(zmap_partitions_path):
        os.makedirs(zmap_partitions_path) # -> create the ZMAP output directory

    #   b. Partitioning Experiment ID
    n_exp = len(os.listdir(zmap_partitions_path))
    zmap_partitions_path += f"/exp_{n_exp}"

    #       if not already present the directory path, create it
    if not os.path.exists(zmap_partitions_path):
        os.makedirs(zmap_partitions_path) # -> create the exp directory



    # 4) split Source Dataset into N csv files (to increase concurrency among multiple Google VMs)

    source_zmap_csv = open(source_zmap_path, 'r').readlines()

    #   partition size (last one will get the remaining entries)
    partitions_size = len(source_zmap_csv) // n_partitions

    #   partitions writing
    for i in range(n_partitions):

        partition_path = f"{zmap_partitions_path}/{i+1}_{n_partitions}.csv"
        # START label
        start = i * partitions_size

        if i == n_partitions-1: # last iteration
            # END label
            end = len(source_zmap_csv)

        else: # other iterations
            # END label
            end = (i+1)*partitions_size
        
        files_handling.store_data(["exp_"+str(n_exp), datetime.date.today(), n_partitions, i+1, end-start, datasets[dataset_id]],
                                  "IpScan/logs/results_partitioned.csv")

        with open(partition_path, 'w+') as out_file:
            # writing one partition considering START and END labels
            out_file.writelines(source_zmap_csv[start:end])

    return None