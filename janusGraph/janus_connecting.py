from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.traversal import Cardinality
from gremlin_python.driver.serializer import GraphSONSerializersV3d0

connection = DriverRemoteConnection('ws://192.168.2.65:8182/gremlin', 'g',
                                    message_serializer=GraphSONSerializersV3d0())
# The connection should be closed on shut down to close open connections with connection.close()
g = traversal().withRemote(connection)
# Reuse 'g' across the application

hercules_age = g.V().has('name', 'hercules').values('age').next()
#hercules_age = g.V().has('person', 'name', 'hercules').values('age').next()
print(f'Hercules is {hercules_age} years old.')



# Upsert by unique 'name'
v = (
    g.V().has('person', 'name', 'user13')
     .fold()
     .coalesce(
         __.unfold(),
         __.addV('person').property(Cardinality.single, 'name', 'user13')
     )
     .next()
)
vid = v.id
print(f"Upserted vertex id: {vid}")

print(g.V(vid).values('name').next())
