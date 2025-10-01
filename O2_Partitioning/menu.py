import datetime

from utils import files_handling

def partitioning_menu():

    # Header
    print('-' * 50, '[PARTITIONING]', '-' * 50)
    # Description
    print("Logical Partitioning of the IP list into N portions to be analysed separately\n")



    '''1) Dataset selection'''

    # level to choose by the user
    level = 'dataset'

    # returns the zmap dataset selected by the user (ex. 01_output)
    zmap_dataset = files_handling.level_selection(level, None)
                                                               
    if zmap_dataset == None:
        # to MAIN MENU
        print("\n\t[Redirection to main menu ...]\n")                                      
        return


    print('*' * 75)



    '''2) print associated available experiments'''

    # getting all experiments info performed on the selected dataset
    experiments = files_handling.get_exps_info(zmap_dataset)

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

    exp_name = f"exp_{len(experiments)+1}"
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



    '''5) LOGs info about just created experiment'''

    # logs info about just created experiment
    files_handling.store_data([exp_name, str(datetime.date.today()), n_partitions, zmap_dataset],
                              "utils/logs/experiments.csv")

    return