# MIT License
#
# Copyright (c) 2018 Evgeny Medvedev, evge.medvedev@gmail.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import click

from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.rpc.bitcoin_rpc import BitcoinRpc

from blockchainetl.logging_utils import logging_basic_config
from blockchainetl.streaming.streaming_utils import configure_logging, configure_signals
from blockchainetl.thread_local_proxy import ThreadLocalProxy

from bitcoinetl.streaming.streaming_utils import get_item_exporter
from bitcoinetl.streaming.btc_streamer_adapter import BtcStreamerAdapter
from blockchainetl.streaming.streamer import Streamer

provider_uri = 'http://bitcoin:passw0rd@localhost:8332'
output = 'kafka/localhost:9092'
chain = Chain.BITCOIN
batch_size = 1
enrich = True
max_workers = 1

streamer_adapter = BtcStreamerAdapter(
    bitcoin_rpc=ThreadLocalProxy(lambda: BitcoinRpc(provider_uri)),
    item_exporter=get_item_exporter(output),
    chain=chain,
    batch_size=batch_size,
    enable_enrich=enrich,
    max_workers=max_workers
)

streamer_adapter.open()
streamer_adapter.export_all(start_block=327600, end_block=327600)
streamer_adapter.close()
