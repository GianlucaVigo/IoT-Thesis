from O1_ZMap.menu import zmap_menu
from O2_Partitioning.menu import partitioning_menu
from O3_GetResource.menu import getresource_menu
from O4_Observe.menu import observe_menu
from O5_Analysis.menu import analysis_menu


def main():

    while(True):
        print('-' * 111)
        print('-' * 50, 'MAIN MENU', '-' * 50)
        print('-' * 111)

        print("What kind of operation you want to perform? (Specify the associated number)\n")
        print("\t 0. [ZMAP] ZMap utilities")
        print("\t 1. [PARTITIONING] partition the IP info list into N partitions to be analysed separately")
        print("\t 2. [GET RESOURCE] interact with CoAP resources and retrieve their content through a GET request")
        print("\t 3. [OBSERVATION] observe the observable resources found and store updates")
        print("\t 4. [ANALYSIS] extract knowledge/insights")
        print("\t 5. [EXIT] exit from the application")
        print("\nOption: ")

        try:
            # user choice interpreted as an integer
            choice = int(input())

        except:
            print("\tINPUT ERROR: Enter a valid option please!")    # Invalid user choice
            continue

        # the program execution passes the control towards the menu associated to user choice
        match choice:
            case 0:
                zmap_menu()                                                 # ZMap utility
                continue

            case 1:
                partitioning_menu()                                         # Partitioning
                continue

            case 2:
                getresource_menu()                                          # Get Resource
                continue

            case 3:
                observe_menu()                                              # Observe
                continue

            case 4:
                analysis_menu()                                             # Analysis
                continue

            case 5:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")     # Invalid user choice
                continue
        
    return None

# call the main function
main()