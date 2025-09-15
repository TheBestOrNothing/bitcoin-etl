from gremlin_python.driver import client, serializer

GREMLIN_ENDPOINT = "ws://192.168.2.65:8182/gremlin"

# Initialize Gremlin client
gclient = client.Client(
    GREMLIN_ENDPOINT,
    'g',
    message_serializer=serializer.GraphSONSerializersV3d0()
)

# Submit query using gclient
try:
    result = gclient.submit(
        "g.V().has('name', 'hercules').values('age')"
    ).all().result()

    if result:
        hercules_age = result[0]
        print(f"Hercules is {hercules_age} years old.")
    else:
        print("Hercules not found.")

finally:
    # Always close the client
    gclient.close()
