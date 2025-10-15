import subprocess
from pathlib import Path

out = "utils/tests/zmap_out.csv"

cmd = [
    "zmap",
    "--target-port=5683",
    "--probe-module=udp",
    "--probe-args=file:utils/zmap/examples/udp-probes/coap_5683.pkt",
    "--bandwidth=1M",
    #"--max-results=5",
    "--output-module=csv",
    "--output-fields=*",
    "--output-file=" + out,
    "0.0.0.0/3"
]

p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

# Print lines as zmap runs (also useful for debug)
try:
    while True:
        line = p.stdout.readline()
        if not line and p.poll() is not None:
            break
        if line:
            print("[zmap]", line.rstrip())
finally:
    p.stdout.close()
    ret = p.wait()

print("zmap exit:", ret)
if Path(out).exists():
    print("Results:", Path(out).read_text())
