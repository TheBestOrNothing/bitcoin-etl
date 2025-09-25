
# Read

gremlin> :remote connect tinkerpop.server conf/remote.yaml session
gremlin> :remote console

Run these **Gremlin Console commands** (they’re just one-liners you can paste step by step):

```groovy
graph = org.janusgraph.core.JanusGraphFactory.open('/opt/janusgraph/conf/janusgraph-cql-server.properties')
g = graph.traversal()
```

```groovy
import org.apache.tinkerpop.gremlin.structure.io.graphson.*
```

```groovy
mapper = GraphSONMapper.build().version(GraphSONVersion.V3_0).create()
writer = GraphSONWriter.build().mapper(mapper).create()
out = new FileOutputStream('conf/blocks10-v3.graphsonl')
```

```groovy
v = g.V().hasLabel('block').limit(1).next()
writer.writeVertex(out, v)
out.write('\n'.getBytes('UTF-8'))
out.close()
```

That produces `/opt/janusgraph/conf/blocks10-v3.graphsonl` (line-delimited `g:Vertex` records).

# Write
gremlin> :install org.apache.tinkerpop hadoop-gremlin 3.7.3
==>Loaded: [org.apache.tinkerpop, hadoop-gremlin, 3.7.3] - restart the console to use [tinkerpop.hadoop]
gremlin> :install org.apache.tinkerpop spark-gremlin 3.7.3
