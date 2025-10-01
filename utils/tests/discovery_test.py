from aiocoap import Message

test = "40017d70b36f696303726573"

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





