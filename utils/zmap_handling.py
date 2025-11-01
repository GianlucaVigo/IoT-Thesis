import subprocess
import os
import datetime

from O1a_DataCollection.menu import lookups as a_lookups
from O1b_DataCollection.menu import lookups as b_lookups

################################################################################################

def zmap_additions(cidr):

    # user can specify some custom ZMap command options
    # bandwidth
    # probes
    # max-results

    user_options = ['bandwidth', 'probes', 'max-results']
    zmap_user_cmd = []

    print("\n-----ZMap-Command-Specifications-----")

    for option in user_options:
        print(f"\tSpecify {option}:", end="")

        user_choice = input()

        # if the user does not provide anything -> the option won't be added to the ZMap command
        if not user_choice: 
            continue
        else:
            zmap_user_cmd.append(f"--{option}={user_choice}")
    
    # append ip range selected by user 
    zmap_user_cmd.append(cidr)

    return zmap_user_cmd

################################################################################################

def zmap_menu(approach):

    # Header + Description
    print('-' * 50, '[ZMAP]', '-' * 50)
    print("Welcome to the ZMAP utility!")

    # NB: 224.0.0.0/3 -> not considered because of the blocklist
    # In my idea one partition for each VM
    internet_portions = ['0.0.0.0/3', '32.0.0.0/3', '64.0.0.0/3', '96.0.0.0/3', '128.0.0.0/3', '160.0.0.0/3', '192.0.0.0/3']

    # base path
    path = f'O1{approach}_DataCollection/discovery/csv'

    print("From which portion of the Internet do you want to start?\n")

    # header
    print("\tindex".ljust(10), "cidr".ljust(19), "daily test".ljust(18))
    # options
    for index, portion in enumerate(internet_portions):
        
        # list of directories available at specified path -> list of DATES
        portion_dates = os.listdir(f"{path}/{index}")
        # getting today date
        today = str(datetime.date.today()) + '.csv'

        # build up the info line to be printed
        info_line = "\t"
        info_line += f"{index}".ljust(10)       # add the index
        info_line += f"{portion}".ljust(20)     # add the portion cidr

        # helper feature to rememeber which portions have been already processed
        if today in portion_dates:
            info_line += "[Done]".ljust(20)     # if there's a 'today' directory -> already done
        else:
            info_line += "[To Do]".ljust(20)    # if there's NOT a 'today' directory -> to be done
    
        print(info_line)
    
    # user selects the portion id
    print("\nPlease select the index:", end="")

    try:
        # get the index and convert it to integer
        internet_portion_id = int(input())
        # retrieve the cidr related
        internet_portion = internet_portions[internet_portion_id]
        
        # ZMap command additions
        cmd = zmap_additions(internet_portion)
        
        if approach == 'a':
            a_lookups(internet_portion_id, cmd, approach)
        elif approach == 'b':
            b_lookups(internet_portion_id, cmd, approach)

    except Exception as e:
        # print the error type
        print(e)

    return

################################################################################################

def execute_zmap(command_additions):

    command = [
        "zmap",
        "--target-port=5683",
        "--probe-module=udp",
        "--blocklist-file=utils/zmap/conf/blocklist.conf",
        "--output-module=csv",
        "--output-fields=*",
        "--output-filter=\"repeat=0\"",
        "-q"
    ]

    command.extend(command_additions)
    
    print(command)

    # ZMap Command Execution
    # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
    # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
    # text=True                 => convert the std output into text (instead of bytes)
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    try:
        # read the process output line-by-line as it runs
        while True:
            # blocks until a newline is available or EOF occurs
            line = p.stdout.readline()

            # line is empty/no data/EOF
            if not line and p.poll() is not None:
                break # exit from the loop
            # otherwise print the captured line
            if line:
                print("[zmap]", line.rstrip())

    finally:
        # close the captured stdout file object 
        p.stdout.close()
        # let the process ends and collect its return code
        ret = p.wait()
        # print return exit
        print("ZMap exit:", ret)
    return

################################################################################################