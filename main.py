from O1_DataCollection.zmap import zmap, after_zmap_execution
from O2_Analysis.menu import analysis_menu

def main():

    while(True):
        print('-' * 111)
        print('-' * 50, 'MAIN MENU', '-' * 50)
        print('-' * 111)

        print("What kind of operation you want to perform? (Specify the associated number)\n")
        print("\t0. [BEFORE ZMAP + ZMAP] Perform data collection")
        print("\t1. [AFTER ZMAP] Perform data collection")
        print("\t2. [ANALYSIS] extract knowledge/insights")
        print("\t3. [EXIT] exit from the application")
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
                zmap()
            
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