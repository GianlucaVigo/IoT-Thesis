from O1a_DataCollection.menu import zmap_menu
from O2_Analysis.menu import analysis_menu


def main():

    while(True):
        print('-' * 111)
        print('-' * 50, 'MAIN MENU', '-' * 50)
        print('-' * 111)

        print("What kind of operation you want to perform? (Specify the associated number)\n")
        print("\t 0. [ZMAP] ZMap utilities")
        print("\t 1. [ANALYSIS] extract knowledge/insights")
        print("\t 2. [EXIT] exit from the application")
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
                analysis_menu()                                             # Analysis
                continue

            case 2:
                print("Bye!")                                               # End the program execution
                break

            case _:
                print("\tINPUT ERROR: Invalid option -> Out of range!")     # Invalid user choice
                continue
        
    return None

# call the main function
main()