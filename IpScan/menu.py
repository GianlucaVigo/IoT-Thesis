import os

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
            print(f"\t{dataset_id}. {datasets[dataset_id]}") 

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
    while(True):
        print("How many csv partitions do you want to create? MAX: 20 ['e' to main menu]")

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

        with open(partition_path, 'w+') as out_file:
            # writing one partition considering START and END labels
            out_file.writelines(source_zmap_csv[start:end])


    return None