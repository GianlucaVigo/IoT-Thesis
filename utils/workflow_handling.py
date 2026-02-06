import maxminddb
import pandas as pd
import os
import datetime
import aiocoap
import asyncio

from utils import payload_handling

from collections import Counter
from aiocoap import *

################################################################################################

# setting MAX_RETRANSMIT = 3
aiocoap.numbers.TransportTuning.MAX_RETRANSMIT = 3

MAX_TRANSMIT_WAIT = (
            aiocoap.numbers.TransportTuning.ACK_TIMEOUT *
            (2 ** (aiocoap.numbers.TransportTuning.MAX_RETRANSMIT + 1) - 1) *
            aiocoap.numbers.TransportTuning.ACK_RANDOM_FACTOR
        )

################################################################################################

def create_file(path, cidr_id, need_new_file, date_and_time):
    
    os.makedirs(path, exist_ok=True)

    
    if cidr_id == None:
        
        if need_new_file:
            
            print("\tFile needs to be created!")
            
            # get current date
            path = os.path.join(path, f"{date_and_time}.csv")
            
            with open(path, "w"):
                pass
        
        else:
            
            print("\tFile already created!")
            
            created_files = os.listdir(path)
            created_files.sort()
            
            # get last file name created
            last_file = created_files[len(created_files) - 1]
            print(f"\tLast file name is {last_file}")
            
            path = os.path.join(path, f"{last_file}")
    
    else:
            
        path = os.path.join(path, f"{cidr_id}.csv")
        
        if need_new_file:

            with open(path, "w"):
                pass
        
        
    return path
        
        
################################################################################################


def avoid_get(row):

    # When can GET requests be avoided?
    
    # ----- CODE check -----
    if row['code'] != '2.05 Content': # not successful discovery (= 2.05 Content)
        return True

    # ----- OPTION check -----
    # Assumption: /.well-known/core resource use the CONTENT FORMAT option equal to LINK FORMAT
    # NB: I filter out everything that is not in LINKFORMAT style (TEXT, ...)
    if row['options'] != None:

        options_dict = eval(row['options'])

        if 'CONTENT_FORMAT' in options_dict.keys():
            if options_dict['CONTENT_FORMAT'] != 'LINKFORMAT':
                return True

    # ----- EMPTY PAYLOAD check -----
    if len(row['data']) == 0: # empty payload string
        return True

    # ----- STILL BINARY PAYLOAD check -----
    if isinstance(row['data'], bytes):
        return True

    return False

################################################################################################

# remove_duplicates()
#   it is able to identify duplicates in the ZMap scan csv files: keep the first, discard the others
#   duplicate = both IP address and data fields equal
def remove_duplicates(df_zmap):

    # [subset=['saddr']] Remove duplicates considering 'saddr' field
    # [keep='first'] Keep the first instance (not delete all duplicates)
    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    df_zmap.drop_duplicates(subset=['saddr'], keep='first', inplace=True)

    # [inplace=True] Whether to modify the DataFrame rather than creating a new one.
    # [drop=True] Do not try to insert index into dataframe columns. This resets the index to the default integer index.
    df_zmap.reset_index(inplace=True, drop=True)

    return df_zmap

################################################################################################

# extract_ip_info()
#   it is able to retrieve IP related information as country, continent, ...
#   it returns a flat dataframe that will be then stored safely
def extract_ip_info(ip_list_df):

    rows = []

    with maxminddb.open_database("utils/ipinfo/ipinfo_lite.mmdb") as reader:
        for ip in ip_list_df["saddr"]:
            record = reader.get(ip) or {}

            row = {
                "asn": record.get("asn"),
                "as_name": record.get("as_name"),
                "as_domain": record.get("as_domain"),
                "continent": record.get("continent"),
                "continent_code": record.get("continent_code"),
                "country": record.get("country"),
                "country_code": record.get("country_code"),
            }

            rows.append(row)

    ip_info_df = pd.DataFrame(rows)

    return pd.concat(
        [ip_list_df.reset_index(drop=True), ip_info_df],
        axis=1
    )


################################################################################################

# perform the resource discovery operation -> GET (<IP> + <URI>)
async def get(ip_address, truncated_decoded_msg, context):
        
    # resource URI to be checked
    uri_to_check = f"coap://{ip_address}:5683{truncated_decoded_msg['uri']}"
    # build the request message
    request = Message(code=GET, uri=uri_to_check)

    try:
        # send the request and obtained the response
        response = await asyncio.wait_for(context.request(request).response, timeout=MAX_TRANSMIT_WAIT + 5)

    except Exception:
        return truncated_decoded_msg

    else:
        
        decoded_message_payload = payload_handling.get_payload(response)

        full_decoded_msg = {
            'uri': truncated_decoded_msg['uri'],
            'version': payload_handling.get_version(response),
            'mtype': payload_handling.get_mtype(response),
            'token': payload_handling.get_token(response),
            'token_length': payload_handling.get_token_length(response),
            'code': payload_handling.get_code(response),
            'mid': payload_handling.get_mid(response),
            'options': payload_handling.get_options(response),
            'observable': payload_handling.get_observe(response, truncated_decoded_msg['uri']),
            'data': decoded_message_payload,
            'data_format': payload_handling.get_payload_format(decoded_message_payload),
            'data_length': payload_handling.get_payload_length(response)
        }

        return full_decoded_msg # success

################################################################################################
# decode_data()
#   I: binary payload (in hex)
#   O: structured object with multiple fields
def decode_data(binary_data, uri):

    # default message to be returned (if any error occurs during the decoding process)
    decoded_msg = {
        'uri': uri,
        'version': None,
        'mtype': None,
        'token': None,
        'token_length': None,
        'code': None,
        'mid': None,
        'options': None,
        'observable': None,
        # maintain the raw payload as is
        'data': binary_data,
        'data_format': None,     
        'data_length': None,
        'user_inserted': None
    }

    # ----- decoding ZMap binary message -----

    try:

        msg = Message.decode(bytes.fromhex(binary_data))

    # unable to decode the message
    except Exception:
        return decoded_msg
    
    # ----------------------------------------

    # update/populate the starting decoded message structure with all retrieved info/data
    
    decoded_message_payload = payload_handling.get_payload(msg)
    
    decoded_msg.update({
        'version': payload_handling.get_version(msg),
        'mtype': payload_handling.get_mtype(msg),
        'token': payload_handling.get_token(msg),
        'token_length': payload_handling.get_token_length(msg),
        'code': payload_handling.get_code(msg),
        'mid': payload_handling.get_mid(msg),
        'options': payload_handling.get_options(msg),
        'observable': payload_handling.get_observe(msg, uri),
        'data': decoded_message_payload,
        'data_format': payload_handling.get_payload_format(decoded_message_payload),
        'data_length': payload_handling.get_payload_length(msg)
    })

    return decoded_msg
        
################################################################################################

# decode()
#   it takes as input the raw/binary ZMap data field and it returns a structured object representing a CoAP response message
#   O: version, message type (mtype), token length, code (response code), mid (message id), tokenn, options, data (payload)
async def decode(df_zmap, uri):

    # it contains all the decoded messages
    new_data_list = []
    # undecodable messages
    undecodable_msgs = []
    # it contains a summary of the decode process (success, unsuccess/<REASON>)
    decode_results = Counter()


    # client context creation for eventual GETs
    context = await Context.create_client_context()

    # iterate over rows
    for _, row in df_zmap.iterrows():

        # logging
        if (len(new_data_list) % 200 == 0):
            print(f"\t({datetime.datetime.now()}) Rows examined: {round(len(new_data_list)/df_zmap.shape[0] * 100, 2)}%")
        
        # default decoded message (it will be returned if any error occurs)
        decoded_msg = {
            'saddr': row['saddr'],
            'uri': None,
            'version': None,
            'mtype': None,
            'token': None,
            'token_length': None,
            'code': None,
            'mid': None,
            'options': None,
            'observable': None,
            'data': None,
            'data_format': None,
            'data_length': None,
            'user_inserted': None
        }

        # if success field is equal to 1 (all UDP kind of result)
        if row['success'] == 1:

            # decoding binary data, get a dictionary as result and update decoded message
            decoded_msg.update(decode_data(row['data'], uri))

            # if decodede message code field is equal to None -> something went wrong during the decoding process
            #   ex. undecodable message, ...
            if decoded_msg['code'] is None:

                # update summary
                decode_results.update([f"undecodable_msg"])
                
                undecodable_msgs.append([decoded_msg['saddr'], decoded_msg['data'], decoded_msg['uri']])

            # otherwise it is a success
            else:

                # update summary
                decode_results.update(['success'])

                # detect if ZMap retrieved payload is truncated
                if payload_handling.detect_truncated_response(row['udp_pkt_size'], row['data'], decoded_msg):

                    decoded_msg.update(await get(row['saddr'], decoded_msg, context))
            
                # success case -> append and store it
                new_data_list.append(decoded_msg)

        # if success field is equal to 0 (all icmp, ... kind of result)
        else:
            decode_results.update([f"unsuccess/{row['icmp_unreach_str']}"])
    
    
    # close context/UDP socket
    await context.shutdown()
    
    # Build dataframe from list
    decoded_df = pd.DataFrame(new_data_list)
        
    # define result to be returned
    result = [decoded_df, decode_results]
    
    # ---------- undecodable-msgs management ----------
    
    # not empty list
    if undecodable_msgs:
        
        try: 
            # convert list of lists into Pandas dataframe
            undecodable_df = pd.DataFrame(undecodable_msgs, columns=['saddr', 'data', 'uri'])
            
            result.append(undecodable_df)
            
        except Exception as e:
            print(e)
            
    else:
        print("\tNo undecodable messages")
        result.append(None)
            
    # -------------------------------------------------

    # return datafram structure + summary
    return result