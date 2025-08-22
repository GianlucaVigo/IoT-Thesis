# Analysis

During this last phase, I am extracting some knowledge out the collected datasets.

Until now, I considered the following types of analysis to be performed:
    - country           IP
        plot the IP's country distribution
    - continent         IP
        plot the IP's continent distribution
    - as_name           IP
        plot the IP's as_name distribution


    - size_stats        PAYLOAD
        plot the Payload Size distribution + some statistical metrics as average and median
    - most_common       PAYLOAD
        plot the most common resource names among all the CoAP servers
    - #_resources       PAYLOAD
        plot the number of resources per CoAP server distribution
    - resources_depth   PAYLOAD
        plot the resource depth (in terms of levels) distribution
            ex. "/boot/info"
                I consider this resource having 3 levels
    - #_coap_servers    PAYLOAD
        plot and print the number of machines which are and aren't CoAP servers


    - stability         GENERAL
        this kind of analysis can be performed only when multiple tests over time have been performed on the same experiment on the same zmap dataset

        the stability analysis focuses on retrieving those IPs that change status over time:
            CoAP -> no CoAP (mainly this)
            no CoAP -> CoAP

        i considered this type of analysis could be interesting in order to prove that usually CoAP devices are often TRANSIENT



