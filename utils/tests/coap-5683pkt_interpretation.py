rel_path= 'utils/zmap/examples/udp-probes/coap_5683.pkt'

with open(rel_path, "rb") as f:
    data = f.read()

    print(f"Length: {len(data)}")

    print(f"Hex: {data.hex()}")

    binary_string = ''.join(format(byte, '08b') for byte in data)
    print(f"Binary: {binary_string}")    