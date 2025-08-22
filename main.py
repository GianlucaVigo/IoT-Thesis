from IpScan.menu import zmap_menu
from DataRefinement.menu import refinement_menu
from Analysis.menu import analysis_menu

# user menu
def main():

    while(True):
        print("What kind of operation you want to perform? (Specify the associated number)")
        print("\t 0. [ZMAP] partition the ZMAP output results into N partitions to be analysed separately")
        print("\t 1. [Data Refinement] testing IP whether being a CoAP server")
        print("\t 2. [Data Refinement] obtain additional info about IP addresses")
        print("\t 3. [Analysis] extract knowledge")
        print("\t 4. [Exit] exit from the application")
        print("Option: ")

        try:
            # user choice saved as integer
            choice = int(input())
            print("-" * 100)

        except:
            print("\tINPUT ERROR: Enter a valid option please!")    # Invalid user choice
            print("-" * 100)
            continue

        # the program execution passes the control towards the menu associated to the user choice
        match choice:
            case 0:
                zmap_menu()                                                 # IpScan
                continue

            case 1:
                refinement_menu("coap")                                     # DataRefinement - CoAP
                continue

            case 2:
                refinement_menu("ip")                                       # DataRefinement - IP
                continue

            case 3:
                analysis_menu()                                             # Analysis
                continue

            case 4:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                print("-" * 100)
                continue
        
    return None

# call the main function
main()