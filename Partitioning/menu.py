import os
from utils import files_handling
import datetime

def partitioning_menu():

    '''1) IpInfo dataset selection'''

    # info available at the moment
    path_source = {'phase': 'IpInfo', 'folder': 'csv'}
    # level chosen by the user
    level = 'dataset'                                   

    # returns the dataset selected by the user (ex. 01_output.csv)
    dataset = files_handling.level_selection(level, path_source)
    
    # IF
    # T: return to 'main menu'                                                           
    if dataset == None:                                             
        return
    # F: update path by adding dataset
    else:                                                           
        path_source.update({level: dataset})
    

    '''2) user defines the number of partitions to be created'''
    # getting all experiments performed on the selected dataset
    experiments = files_handling.get_exps_info(dataset)

    if len(experiments) == 0: # no experiments available
        print("\tNo experiments available!\n")
    
    else:
        print("Here's the list of experiments created over the selected dataset:\n")

        # iterating over the experiment dictionaries
        for i, exp in enumerate(experiments):
            # print index                         
            exp_info_row = f"\t{i:2d}. "                                

            # print metadata info (exp_name, date, n_partitions)
            for key, value in exp.items():                              
                exp_info_row += f" {key}: {value} |"

            print(exp_info_row[:-1])

    

    while(True):
        # new experiment?                                                   
        print("\nDo you want to create a NEW experiment? [Y/n]")

        # user input
        answer = input()                                            

        match answer:
            # Create/Define a new experiment over the selected dataset
            case "Y":                                               
                break

            # To main menu
            case "n":
                print("\t[Redirection to main menu ...]")           
                print("-" * 100)
                return
                
            # Invalid user choice
            case _:
                print("\tINPUT ERROR: Invalid input")               
                print("-" * 100)


    while(True):
        print("\nHow many csv partitions do you want to create? MAX: 20 ['e' to main menu]")

        # upperbound limit over the number of partitions to be created
        n_partitions_upperbound = 20                                

        try:
            # user input: number of partitions to be created
            choice = input()                                        
            print("-" * 100)

            # -> to MAIN MENU
            if (choice == 'e'):
                return                                              

            # convert the user input: str -> int   
            n_partitions = int(choice)                              

        # Invalid user input
        except Exception as e:
            print("\tINPUT ERROR: Invalid input")                   
            print(f"\t\t {e}")
            print("-" * 100)
            continue

        # Invalid user choice
        if (n_partitions > n_partitions_upperbound):
            print("\tINPUT ERROR: Invalid option -> Too many partitions!")    
            print("-" * 100)
            continue
            
        # Invalid user choice
        if (n_partitions <= 0):
            print("\tINPUT ERROR: Invalid option -> Number of partitions can't be zero or negative!")    
            print("-" * 100)
            continue
            
        break


    '''3) directory structure preparation'''

    # Source Dataset selection
    path_partitions = {'phase': 'Partitioning', 'folder': 'csv', 'dataset': dataset}
    path_partitions_str = files_handling.path_dict_to_str(path_partitions)

    # if not already present the directory path...
    if not os.path.exists(path_partitions_str):
        # create the ZMAP output directory
        os.makedirs(path_partitions_str) 

    # Partitioning Experiment ID
    n_exp = len(os.listdir(path_partitions_str))
    path_partitions.update({'experiment': f"exp_{n_exp}"})
    path_partitions_str = files_handling.path_dict_to_str(path_partitions)

    # if not already present the directory path...
    if not os.path.exists(path_partitions_str):
        # create the exp directory
        os.makedirs(path_partitions_str)



    # 4) split Source Dataset into N csv files (to increase concurrency among multiple Google VMs)
    source_zmap_csv = open(files_handling.path_dict_to_str(path_source), 'r').readlines()

    #   partition size (last one will get the remaining entries)
    partitions_size = len(source_zmap_csv) // n_partitions

    #   partitions writing
    for i in range(n_partitions):

        path_partitions.update({'partition': f"{i+1}_{n_partitions}.csv"})

        # START label
        start = i * partitions_size

        if i == n_partitions-1: # last iteration
            # END label
            end = len(source_zmap_csv)

        else: # other iterations
            # END label
            end = (i+1)*partitions_size
        
        files_handling.store_data(["exp_"+str(n_exp), i+1, end-start, dataset],
                                  "utils/logs/partitions.csv")

        with open(files_handling.path_dict_to_str(path_partitions), 'w+') as out_file:
            # writing one partition considering START and END labels
            out_file.writelines(source_zmap_csv[start:end])

    files_handling.store_data(["exp_"+str(n_exp), datetime.date.today(), n_partitions, dataset],
                              "utils/logs/experiments.csv")

    return None