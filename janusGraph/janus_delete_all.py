from gremlin_python.driver import client, serializer

# ---- connect ----
GREMLIN_ENDPOINT = "ws://192.168.2.65:8182/gremlin"
gclient = client.Client(GREMLIN_ENDPOINT, 'g',
                        message_serializer=serializer.GraphSONSerializersV3d0())


def _install_delete_fn(gremlin_client):
    """
    Installs a server-side Groovy helper: deleteBlockByNumber(height).
    Deletes:
      - outputs created by txs in the block (and their pay_to edges)
      - txs in the block (and their belongs_to edges)
      - coinbase edge from block (if any)
      - the block vertex
    Returns a summary map with counts.
    """
    script = """
    def deleteBlockByNumber(height) {
      def blocks = g.V().hasLabel('block').has('block_number', height).toList()
      if (blocks.isEmpty()) return ['blocks': 0L, 'txs': 0L, 'outputs': 0L, 'pay_to_edges': 0L, 'belongs_to_edges': 0L, 'coinbase_edges': 0L]

      long bcount=0L, tcount=0L, ocount=0L, payedges=0L, beledges=0L, coinedges=0L

      // handle all competing blocks at this height (forks)
      blocks.each { b ->
        // transactions included in this block
        def txs = g.V(b).in('belongs_to').toList()
        tcount += txs.size()

        // outputs created by those transactions
        def outs = g.V(txs).out('lock_to').toList()
        ocount += outs.size()

        // count edges for reporting, then drop:
        if (!outs.isEmpty()) {
          payedges += g.V(outs).outE('pay_to').count().tryNext().orElse(0)
          g.V(outs).outE('pay_to').drop().iterate()
          // drop outputs (spent_in edges go with them)
          g.V(outs).drop().iterate()
        }

        if (!txs.isEmpty()) {
          beledges += g.V(txs).outE('belongs_to').count().tryNext().orElse(0)
          g.V(txs).outE('belongs_to').drop().iterate()
          g.V(txs).drop().iterate()
        }

        coinedges += g.V(b).outE('coinbase').count().tryNext().orElse(0)
        g.V(b).outE('coinbase').drop().iterate()

        // finally, drop the block
        g.V(b).drop().iterate()
        bcount += 1L
      }

      return ['blocks': bcount, 'txs': tcount, 'outputs': ocount,
              'pay_to_edges': payedges, 'belongs_to_edges': beledges, 'coinbase_edges': coinedges]
    }
    """
    gremlin_client.submit(script).all().result()

def delete_block_by_number(block_number: int, ensure_from_tip: bool = True):
    """
    Deletes all blocks at the given block_number (handle forks),
    their transactions, outputs, and related edges — keeps address vertices.
    Set ensure_from_tip=True to guard against non-tip deletion.
    """
    print(f"[START] delete_block_by_number(height={block_number})")

    c = client.Client(GREMLIN_ENDPOINT, "g",
                      message_serializer=serializer.GraphSONSerializersV3d0())

    try:
        # Optional safety: ensure we’re deleting from tip downward
        if ensure_from_tip:
            max_h = c.submit(
                "g.V().hasLabel('block').values('block_number').max()"
            ).all().result()
            max_h = max_h[0] if max_h else None
            print(f"[INFO] Current max block_number on graph: {max_h}")
            if max_h is None:
                print("[WARN] No blocks present; nothing to delete.")
                print(f"[END] delete_block_by_number(height={block_number})")
                c.close()
                return
            if block_number != max_h:
                print(f"[ABORT] Refusing to delete height {block_number} because tip is {max_h}. "
                      f"Pass ensure_from_tip=False to override (NOT RECOMMENDED).")
                print(f"[END] delete_block_by_number(height={block_number})")
                c.close()
                return

        # Install helper once (idempotent)
        _install_delete_fn(c)

        # Execute deletion
        summary = c.submit("deleteBlockByNumber(h)", {"h": int(block_number)}).all().result()
        stats = summary[0] if summary else {}
        print("[RESULT] Deletion summary:", stats)

    finally:
        c.close()
        print(f"[END] delete_block_by_number(height={block_number})")


# -------- Example usage --------
if __name__ == "__main__":
    # Delete the current tip block (e.g., 902345). Make sure it’s the tip or set ensure_from_tip=False consciously.
    delete_block_by_number(0, ensure_from_tip=True)

