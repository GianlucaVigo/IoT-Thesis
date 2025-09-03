# Data Refinement

The "Data Refinement" phase is in charge of two main processes:

    1) starting from the IP list provided by the ZMAP tool, establish if an IP address is related to a machine exposing a CoAP service.
    HOW?
    By sending a Resource Discovery request:
    GET (./well-known/core) on port 5683 req

        -> performed in "coap.py" module
            -> aiocoap library
        -> this process will be performed by <N> (as the number of partitions created) Google VMs in order to optimize/reduce total time

        -> INPUT: zmap output CSV partition [IPaddress]
        -> OUTPUT: CSV file containing additional info obtained from server response 
                [ IPaddress | Code | Mtype | Payload | PayloadSize ]
                These results are place in the "results_partitioned/<ZMAP_dataset>/<EXP_NUM>" folder

    2) once ALL the partitions of an experiment have been processed, we can pass to this second phase.

    Those IP addresses considered VALID (= got a response from them) will be passed as parameters of a REST API [ipinfo.io].

    This API is able to retrieve more info about an IP address for free:
        [ 'asn', 'as_name', 'as_domain', 'country_code', 'country', 'continent_code', 'continent' ]

    -> performed in "ip.py" module
        -> ip info API

    -> INPUT: many CSV partitions files obtained at previous point
    -> OUTPUT: one CSV file containing all collected information named as "results/<ZMAP_dataset>/<EXP_NUM>/<PROCESS_DATETIME>.csv"
        => this last file is the one that will be used for subsequent analysis