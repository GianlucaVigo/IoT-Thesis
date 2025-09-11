import os
import datetime

from utils import files_handling

def partitioning_menu():

    # Header
    print('-' * 50, '[PARTITIONING]', '-' * 50)
    # Description
    print("Partition the IP info list into N partitions to be analysed separately\n")


    '''1) IpInfo dataset selection'''
    # level to choose by the user
    level = 'dataset'

    # path_source = IpInfo/csv/
    path_source = {'phase': 'IpInfo', 'folder': 'csv'}
                                     

    # returns the ipinfo dataset selected by the user (ex. 01_output.csv)
    ipinfo_dataset = files_handling.level_selection(level, path_source)
                                                               
    if ipinfo_dataset == None:
        # to MAIN MENU
        print("\n\t[Redirection to main menu ...]\n")                                      
        return 
    else: 
        # path_source = IpInfo/csv/01_output.csv                                                          
        path_source.update({level: ipinfo_dataset})
    


    print('*' * 75)
    '''2) print available experiments'''
    # getting all experiments info performed on the selected dataset
    experiments = files_handling.get_exps_info(ipinfo_dataset)

    if len(experiments) == 0:
        print("\n\tNo experiments available!\n")
    
    else:
        print("\nHere's the list of experiments created over the selected dataset:\n")

        # printing header
        print("\tindex".ljust(10), end="")
        for key in experiments[0].keys():
            print(key.ljust(20), end="")
        print()

        # printing list of already created experiments
        for exp_id, experiment in enumerate(experiments):
            print(f"\t{exp_id}".ljust(10), end="")
            try:
                for exp_info in experiment.values():
                    print(str(exp_info).ljust(20), end="")
            except Exception as e:
                print(f"\t[ERROR] No metadata have been collected yet\n\t\t{e}")
            print()
    
    print()

    

    '''3) user choose whether create or not a new experiment'''
    while(True):

        print('*' * 75)

        # new experiment?                                                   
        print("\nDo you want to create a NEW experiment? [Y/n]")

        # user input
        answer = input()                                            

        match answer:
            # "Y": Create/Define a new experiment over the selected dataset
            case "Y":                                               
                break

            # "n": to MAIN MENU
            case "n":
                print("\n\t[Redirection to main menu ...]\n")           
                return
                
            # Default Case: Invalid user choice
            case _:
                print("\n\tINPUT ERROR: Invalid input\n")               

    print()


    '''4) user defines the number of partitions to be created'''
    while(True):

        print('*' * 75)

        print("\nHow many csv partitions do you want to create? MAX: 20 ['e' to main menu]")

        # upperbound limit over the number of partitions to be created
        n_partitions_upperbound = 20                                

        try:
            # user input: number of partitions to be created
            choice = input()                                        

            # -> to MAIN MENU
            if (choice == 'e'):
                print("\n\t[Redirection to main menu ...]\n")           
                return                                              

            # convert the user input: str -> int   
            n_partitions = int(choice)                              

        # Invalid user input
        except Exception as e:
            print("\n\tINPUT ERROR: Invalid input\n")                   
            print(f"\t\t {e}")
            continue

        # Invalid user choice
        if (n_partitions > n_partitions_upperbound):
            print("\n\tINPUT ERROR: Invalid option -> Too many partitions!\n")    
            continue
            
        # Invalid user choice
        if (n_partitions <= 0):
            print("\n\tINPUT ERROR: Invalid option -> Number of partitions can't be zero or negative!\n")    
            continue
            
        break

    print()


    '''5) directory structure preparation'''
    # Source Dataset selection
    path_partitions = {'phase': 'Partitioning', 'folder': 'csv', 'dataset': ipinfo_dataset}
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



    '''6) split Source Dataset into N csv files (to increase concurrency among multiple Google VMs)'''
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
        
        # logs info about current partition
        files_handling.store_data(["exp_"+str(n_exp), i+1, end-start, ipinfo_dataset],
                                  "utils/logs/partitions.csv")

        with open(files_handling.path_dict_to_str(path_partitions), 'w+') as out_file:
            # writing one partition considering START and END labels
            out_file.writelines(source_zmap_csv[start:end])

    # logs info about just created experiment
    files_handling.store_data(["exp_"+str(n_exp), datetime.date.today(), n_partitions, ipinfo_dataset],
                              "utils/logs/experiments.csv")

    return None