from IpInfo.menu import ipinfo_menu
from Partitioning.menu import partitioning_menu
from Discovery.menu import discovery_menu
from GetResource.menu import getresource_menu
from Observe.menu import observe_menu
from Merging.menu import merging_menu
from Analysis.menu import analysis_menu

# user menu
def main():

    while(True):
        print('-' * 111)
        print('-' * 50, 'MAIN MENU', '-' * 50)
        print('-' * 111)

        print("What kind of operation you want to perform? (Specify the associated number)\n")
        print("\t 0. [IP INFO] obtain additional info about collected IP addresses")
        print("\t 1. [PARTITIONING] partition the IP info list into N partitions to be analysed separately")
        print("\t 2.\t [DISCOVERY] perform resource discovery among found CoAP servers")
        print("\t 3.\t\t [GET RESOURCE] interact with CoAP resources and retrieve their content through a GET request")
        print("\t 4.\t\t [OBSERVATION] observe the observable resources found and store updates")
        print("\t 5. [MERGING] merge partitions of a particular date, experiment and dataset")
        print("\t 6. [ANALYSIS] extract knowledge/insights")
        print("\t 7. [EXIT] exit from the application")
        print("\nOption: ")

        try:
            # user choice interpreted as an integer
            choice = int(input())

        except:
            print("\tINPUT ERROR: Enter a valid option please!")    # Invalid user choice
            continue

        # the program execution passes the control towards the menu associated to the user choice
        match choice:
            case 0:
                ipinfo_menu()                                               # Ip Info
                continue

            case 1:
                partitioning_menu()                                         # Partitioning
                continue

            case 2:
                discovery_menu()                                            # Resource Discovery
                continue

            case 3:
                getresource_menu()                                          # Get Resource
                continue

            case 4:
                print('-' * 50, '[OBSERVATION]', '-' * 50)
                observe_menu()                                              # Observe
                continue

            case 5:
                merging_menu()                                              # Merging
                continue

            case 6:
                analysis_menu()                                             # Analysis
                continue

            case 7:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")     # Invalid user choice
                continue
        
    return None

# call the main function
main()