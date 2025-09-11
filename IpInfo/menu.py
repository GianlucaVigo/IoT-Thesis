from IpInfo.ip import findIpInfo
from utils import files_handling


def ipinfo_menu():

    # Header
    print('+' * 50, '[IP INFO]', '+' * 50)
    # Description
    print("Obtain additional info about collected IP addresses:\n\t- asn\n\t- as_name\n\t- as_domain\n\t- country_code\n\t- country\n\t- continent_code\n\t- continent\n")

    # 1) ZMAP dataset selection
    # Levels to choose:
    #   0. zmap dataset (ex. 01_output.csv)
    level = 'dataset'
    
    # path = zmap/csv/
    path = {'phase': 'zmap', 'folder': 'csv'}

    # zmap_dataset selection
    zmap_dataset = files_handling.level_selection(level, path)

    if zmap_dataset != None:
        # path = zmap/csv/01_output.csv
        path.update({level: zmap_dataset})
        # 2) find IP info
        findIpInfo(path)                
    
    return