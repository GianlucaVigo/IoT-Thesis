# IpScan


## ZMAP
To scan the entire Internet looking for servers with 5683 open port, I'm using ZMAP tool.

I have installed it via the "Building from source" option and it works.

In particular, I executed the following command (at home) in the zmap folder:

    $ sudo zmap -M udp -o "04_output.csv" -p 5683 --probe-args=file:examples/udp-probes/coap_5683.pkt -B 80M

so in the csv file "<n>_output.csv" I have all the IP addresses having the 5683 port open.


## Scans list
Here's the list of scans, I run until now:

01_output.csv | 22/07/25 | 15:49 -> 16:19 |  30 min  | #IPs = 42 | run on my home network
02_output.csv | 31/07/25 | 11:16 -> 12:27 | 1h 11min |#IPs = 99 | run on my home network

I'm also interested in monitoring these resources STABILITY over time, so over the same dataset I'll perform the Data Refinement option multiple times.


## IpScan directory
In the IpScan directory, there are different elements:

- "menu.py" file:
    the zmap_menu() function performs the following operations:
        1) source zmap dataset selection (taken from "results" dir)
        2) user defines the number of partitions to be created
        3) directory structure implementation (inside "results_partitioned" folder, create a directory names as the zmap source file selected. Then place a dir named exp_<N> to define a new test/experiment to be performed)
        4) split Source Dataset into N csv files WITHOUT header (to increase concurrency among multiple Google VMs). The last partition will get all the remaining IP addresses.

- "logs" directory:
    contains the zmap processes log.

- "results" directory:
    contains csv files which are the output of the zmap process.
        ex. 01_output.csv

- "results_partitioned" directory:
    contains different folders related to the experiments, performed over the same zmap dataset, named incrementally.
    Partition names are defined in the following way:
    1_5.csv -> partition-id_#-partitions.csv
            -> 1of5.csv
        ex.
            01_output.csv
                exp_0
                    1_3.csv
                    2_3.csv
                    3_3.csv
                exp_1
                ...
            02_output.csv
                exp_0
                exp_1
                exp_2
                ...