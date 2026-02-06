from O1_DataCollection.zmap import balance_zmap_datasets, after_zmap_execution
from O2_Analysis.menu import analysis_menu

def main():

    while(True):
        print('-' * 51)
        print('-' * 20, 'MAIN MENU', '-' * 20)
        print('-' * 51)

        print("What kind of operation you want to perform? (Specify the associated number)\n")
        print("\t0. Balance ZMap Raw Datasets")
        print("\t1. Refine ZMap Raw Datasets")
        print("\t2. Extract knowledge/insights out of collected and refined data")
        print("\t3. Exit from the application")
        print("\nOption: ", end="")

        try:
            # user choice interpreted as an integer
            choice = int(input())

        except:
            print("\tINPUT ERROR: Enter a valid option please!")    # Invalid user choice
            continue

        # the program execution passes the control towards the menu associated to user choice
        match choice:
            
            case 0:
                balance_zmap_datasets()
            
            case 1:
                after_zmap_execution()

            case 2:
                analysis_menu()                                             # Analysis
                continue

            case 3:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")     # Invalid user choice
                continue
        
    return None

# call the main function
main()