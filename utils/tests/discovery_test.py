from aiocoap import Message

test = "82624585d85de20b65ce6e321365c63b73f09e57a8463832fe7752ed5e666320feefb4dbdf94e7bbfee67da5947111753b8cb22c4530dd7484927a3970e543503160ceaa5925053ac90f623fa3723734a4297609ee6c1e0bbe729608a3a0d64b648f59d27e3b86e579c42b34638f553f3799408bc26ebb6a91bd2dc4c044f20c3e8259c7dc95e11e7e7c5cd0f04c42384387454f6d0255349b9fa22afe8081679976b2f1bc4d9efe13a808194760a94711c76cab0b604148bde14ef43949487a429fafc1acc8ea0a9c21a8e11f70d4eb5b34c7d28add9e0b1399da1a187ec0db4b356ed1343ae8ee9bd54781f54079047ce3f223444f08e745db048b1b35a5d7e351d72c5b8e5013f7a02b12d74605d31b"

msg = Message.decode(bytes.fromhex(test))

print(msg)

print(f"Version: {msg.version}")
print(f"Type: {msg.mtype}")
print(f"Token Length: {len(msg.token)}")
print(f"Code: {msg.code}")
print(f"Message ID: {msg.mid}")
print(f"Token: {msg.token.decode('utf-8')}")
for option in msg.opt.option_list():
    print(option)

for option in msg.opt._options:
    print(option)
print(f"Payload: {msg.payload.decode('utf-8')}")
print(f"Payload Length: {len(msg.payload.decode('utf-8'))}")





