from IpInfo.ip import findIpInfo
from utils import files_handling


def ipinfo_menu():

    # 1) ZMAP dataset selection
    # Levels:
    #   0. zmap dataset (ex. 01_output.csv)

    levels = ['dataset']
    path = {'phase': 'zmap', 'folder': 'csv'}

    # path = zmap/csv/

    for level in levels:
        choice = files_handling.level_selection(level, path)

        if choice == None:
            return
        else: 
            path.update({level: choice})

    # path = zmap/csv/01_output.csv
                
    # 2) find IP info
    findIpInfo(path)

    print('-'*100)