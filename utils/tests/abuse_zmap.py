import subprocess


# ZMap command definition
cmd = [
    "zmap",
    "--target-port=5683",
    "--probe-module=udp",
    "--blocklist-file=utils/zmap/conf/blocklist.conf",
    "--allowlist-file=utils/tests/ips.csv",
    
    "--probe-args=hex:40017d706054696e666f",
    
    "--output-module=csv",
    "--output-fields=*",
    #"-q",
    "--output-file=utils/tests/zmap_out1.csv"
]
    
#################################################

    # ZMap Command Execution
    # stdout=subprocess.PIPE    => captures the process's std output so that Python can read it
    # stderr=subprocess.STDOUT  => redirect the std error to output and so to Python
    # text=True                 => convert the std output into text (instead of bytes)
p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


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


'''def string_to_hex(input_string):
    # First, encode the string to bytes
    bytes_data = input_string.encode('utf-8')

    # Then, convert bytes to hex
    hex_data = bytes_data.hex()

    return hex_data

# Example usage
original_text = "info"
hex_result = string_to_hex(original_text)
print(f"--probe-args=hex:{hex_result}")'''