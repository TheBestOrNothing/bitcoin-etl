# MIT License
#
# Copyright (c) 2018 Omidiora Samuel, samparsky@gmail.com
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

from bitcoinetl.btc_utils import bitcoin_to_satoshi
from bitcoinetl.domain.transaction import BtcTransaction
from bitcoinetl.domain.transaction_input_output import BtcTransactionInputOutput
from bitcoinetl.mappers.join_split_mapper import BtcJoinSplitMapper
from bitcoinetl.mappers.transaction_input_mapper import BtcTransactionInputMapper
from bitcoinetl.mappers.transaction_output_mapper import BtcTransactionOutputMapper


# http://chainquery.com/bitcoin-api/getblock
# http://chainquery.com/bitcoin-api/getrawtransaction
class BtcTransactionInputOutputMapper(object):

    def dict_to_inputoutput(self, dict):
        """Convert a dictionary to an InputsOutputs object."""
        obj = BtcTransactionInputOutput()

        # ---- Inputs fields ----
        obj.i_transaction_hash = dict.get('i_transaction_hash')
        obj.i_input_index = dict.get('i_input_index')
        obj.i_block_hash = dict.get('i_block_hash')
        obj.i_block_number = dict.get('i_block_number')
        obj.i_block_timestamp = dict.get('i_block_timestamp')
        obj.i_spending_transaction_hash = dict.get('i_spending_transaction_hash')
        obj.i_spending_output_index = dict.get('i_spending_output_index')
        obj.i_script_asm = dict.get('i_script_asm')
        obj.i_script_hex = dict.get('i_script_hex')
        obj.i_sequence = dict.get('i_sequence')
        obj.i_required_signatures = dict.get('i_required_signatures')
        obj.i_type = dict.get('i_type')
        obj.i_addresses = dict.get('i_addresses', [])
        obj.i_value = dict.get('i_value')

        # ---- Outputs fields ----
        obj.o_transaction_hash = dict.get('o_transaction_hash')
        obj.o_output_index = dict.get('o_output_index')
        obj.o_block_hash = dict.get('o_block_hash')
        obj.o_block_number = dict.get('o_block_number')
        obj.o_block_timestamp = dict.get('o_block_timestamp')
        obj.o_spent_transaction_hash = dict.get('o_spent_transaction_hash')
        obj.o_spent_input_index = dict.get('o_spent_input_index')
        obj.o_spent_block_hash = dict.get('o_spent_block_hash')
        obj.o_spent_block_number = dict.get('o_spent_block_number')
        obj.o_spent_block_timestamp = dict.get('o_spent_block_timestamp')
        obj.o_script_asm = dict.get('o_script_asm')
        obj.o_script_hex = dict.get('o_script_hex')
        obj.o_required_signatures = dict.get('o_required_signatures')
        obj.o_type = dict.get('o_type')
        obj.o_addresses = dict.get('o_addresses', [])
        obj.o_value = dict.get('o_value')
        obj.o_is_coinbase = dict.get('o_is_coinbase', False)

        # ---- Revision ----
        obj.revision = dict.get('revision')

        return obj

    def inputoutput_to_dict(self, obj):
        """Convert an InputsOutputs object to a dictionary."""
        return {
            'type': 'input_output',
            # ---- Inputs fields ----
            'i_transaction_hash': obj.i_transaction_hash,
            'i_input_index': obj.i_input_index,
            'i_block_hash': obj.i_block_hash,
            'i_block_number': obj.i_block_number,
            'i_block_timestamp': obj.i_block_timestamp,
            'i_spending_transaction_hash': obj.i_spending_transaction_hash,
            'i_spending_output_index': obj.i_spending_output_index,
            'i_script_asm': obj.i_script_asm,
            'i_script_hex': obj.i_script_hex,
            'i_sequence': obj.i_sequence,
            'i_required_signatures': obj.i_required_signatures,
            'i_type': obj.i_type,
            'i_addresses': obj.i_addresses,
            'i_value': obj.i_value,

            # ---- Outputs fields ----
            'o_transaction_hash': obj.o_transaction_hash,
            'o_output_index': obj.o_output_index,
            'o_block_hash': obj.o_block_hash,
            'o_block_number': obj.o_block_number,
            'o_block_timestamp': obj.o_block_timestamp,
            'o_spent_transaction_hash': obj.o_spent_transaction_hash,
            'o_spent_input_index': obj.o_spent_input_index,
            'o_spent_block_hash': obj.o_spent_block_hash,
            'o_spent_block_number': obj.o_spent_block_number,
            'o_spent_block_timestamp': obj.o_spent_block_timestamp,
            'o_script_asm': obj.o_script_asm,
            'o_script_hex': obj.o_script_hex,
            'o_required_signatures': obj.o_required_signatures,
            'o_type': obj.o_type,
            'o_addresses': obj.o_addresses,
            'o_value': obj.o_value,
            'o_is_coinbase': obj.o_is_coinbase,

            # ---- Revision ----
            'revision': obj.revision
        }
