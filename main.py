from IpInfo.menu import ipinfo_menu
from Partitioning.menu import partitioning_menu
from CoapInfo.menu import coap_menu
from Merging.menu import merging_menu
from Analysis.menu import analysis_menu

# user menu
def main():

    while(True):
        print("What kind of operation you want to perform? (Specify the associated number)")
        print("\t 0. [IP Info] obtain additional info about collected IP addresses")
        print("\t 1. [Partitioning] partition the IP address list into N partitions to be analysed separately")
        print("\t 2. [CoAP] testing IP whether being a CoAP server")
        print("\t 3. [Merging] merge partitions of a particular date, experiment and dataset")
        print("\t 4. [Analysis] extract knowledge/insights")
        print("\t 5. [Exit] exit from the application")
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
                ipinfo_menu()                                              # Ip Info
                continue

            case 1:
                partitioning_menu()                                         # Partitioning
                continue

            case 2:
                coap_menu()                                                 # CoAP
                continue

            case 3:
                merging_menu()                                              # Merging
                continue

            case 4:
                analysis_menu()                                             # Analysis
                continue

            case 5:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")    # Invalid user choice
                print("-" * 100)
                continue
        
    return None

# call the main function
main()