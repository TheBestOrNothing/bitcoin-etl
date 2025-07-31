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


#from domain.transaction_input_output import BtcTransactionInputOutput
#print(BtcTransactionInputOutput)
#from transaction_input_output import BtcTransactionInputOutput
#from transaction_input import BtcTransactionInputs


from bitcoinetl.enumeration.chain import Chain
from bitcoinetl.mappers.transaction_mapper import BtcTransactionMapper
from bitcoinetl.domain.transaction_input_output import BtcTransactionInputOutput
from bitcoinetl.mappers.transaction_input_output_mapper import BtcTransactionInputOutputMapper
from bitcoinetl.service.btc_service import BtcService
from blockchainetl.executors.batch_work_executor import BatchWorkExecutor
from blockchainetl.jobs.base_job import BaseJob
from blockchainetl.utils import dynamic_batch_iterator


# Add required_signatures, type, addresses, and value to transaction inputs
class EnrichInputOutputJob(BaseJob):
    def __init__(
            self,
            transactions_iterable,
            batch_size,
            bitcoin_rpc,
            max_workers,
            item_exporter,
            chain=Chain.BITCOIN):
        self.transactions_iterable = transactions_iterable
        self.btc_service = BtcService(bitcoin_rpc, chain)

        self.batch_size = batch_size
        self.batch_work_executor = BatchWorkExecutor(batch_size, max_workers, exponential_backoff=False)
        self.item_exporter = item_exporter

        self.transaction_mapper = BtcTransactionMapper()
        self.ios_mapper = BtcTransactionInputOutputMapper()

    def _start(self):
        self.item_exporter.open()

    def _export(self):
        self.batch_work_executor.execute(self.transactions_iterable, self._enrich_inputs_outputs)

    def _enrich_transactions1(self, transactions):
        transactions = [self.transaction_mapper.dict_to_transaction(transaction) for transaction in transactions]

        all_inputs = [transaction.inputs for transaction in transactions]
        flat_inputs = [input for inputs in all_inputs for input in inputs]

        for transaction_input_batch in dynamic_batch_iterator(flat_inputs, lambda: self.batch_size):
            input_transactions_map = self._get_input_transactions_as_map(transaction_input_batch)
            for input in transaction_input_batch:
                output = self._get_output_for_input(input, input_transactions_map) \
                    if input.spent_transaction_hash is not None else None
                if output is not None:
                    input.required_signatures = output.required_signatures
                    input.type = output.type
                    input.addresses = output.addresses
                    input.value = output.value

        for transaction in transactions:
            self.item_exporter.export_item(self.transaction_mapper.transaction_to_dict(transaction))

    def _enrich_inputs_outputs(self, transactions):
        transactions = [self.transaction_mapper.dict_to_transaction(transaction) for transaction in transactions]
        # Create a new BtcTransactionInputOutput object for each transaction

        for transaction in transactions:
            # inputs_outputs is short for ios
            ios = []
            all_inputs = transaction.inputs
            flat_inputs = [input for input in all_inputs]

            spending_transactions_map = self._get_spending_transactions_as_map(flat_inputs)
            for input in flat_inputs:
                io = BtcTransactionInputOutput()
                # ---- Map INPUT fields 1 ----
                io.i_transaction_hash = transaction.hash
                io.i_input_index = input.index
                io.i_block_hash = transaction.block_hash
                io.i_block_number = transaction.block_number
                io.i_block_timestamp = transaction.block_timestamp
                io.i_spending_transaction_hash = input.spent_transaction_hash
                io.i_spending_output_index = input.spent_output_index
                io.i_script_asm = input.script_asm
                io.i_script_hex = input.script_hex
                io.i_sequence = input.sequence

                spending_transaction = spending_transactions_map.get(input.spent_transaction_hash)
                if spending_transaction is None:
                    raise ValueError('Spending transaction with hash {} not found'.format(input.spent_transaction_hash))

                spent_output_index = input.spent_output_index
                if spending_transaction.outputs is None or len(spending_transaction.outputs) < (spent_output_index + 1):
                    raise ValueError(
                        'There is no output with index {} in transaction with hash {}'.format(
                            spent_output_index, input.spent_transaction_hash))
                        
                output = spending_transaction.outputs[spent_output_index]

                # ---- Map OUTPUT fields 1 ----
                io.o_transaction_hash = spending_transaction.hash
                io.o_output_index = spent_output_index
                io.o_block_hash = spending_transaction.block_hash
                io.o_block_number = spending_transaction.block_number
                io.o_block_timestamp = spending_transaction.block_timestamp
                io.o_spent_transaction_hash = transaction.hash
                io.o_spent_input_index = input.index
                io.o_spent_block_hash = transaction.block_hash
                io.o_spent_block_number = transaction.block_number
                io.o_spent_block_timestamp = transaction.block_timestamp
                io.o_script_asm = output.script_asm
                io.o_script_hex = output.script_hex

                # ---- Map OUTPUT fields 2 ----
                io.o_required_signatures = output.required_signatures
                io.o_type = output.type
                io.o_addresses = output.addresses
                io.o_value = output.value

                # ---- Map INPUT fields 2 ----
                io.i_required_signatures = output.required_signatures
                io.i_type = output.type
                io.i_addresses = output.addresses
                io.i_value = output.value

                # ---- Coinbase field ----
                io.o_is_coinbase = spending_transaction.is_coinbase
                # ---- Revision ----
                io.revision = 1  # Assuming revision is not used, set to 1

                # Add the input-output object to the list
                ios.append(io)
                print()
                print('Input-Output: ', self.ios_mapper.inputoutput_to_dict(io))
                
            for io in ios:
                self.item_exporter.export_item(self.ios_mapper.inputoutput_to_dict(io))

    
    def _get_spending_transactions_as_map(self, transaction_inputs):
        spending_transaction_hashes = [input.spent_transaction_hash for input in transaction_inputs
                              if input.spent_transaction_hash is not None]

        spending_transaction_hashes = set(spending_transaction_hashes)
        if len(spending_transaction_hashes) > 0:
            transactions = self.btc_service.get_transactions_by_hashes(spending_transaction_hashes)
            return {transaction.hash: transaction for transaction in transactions}
        else:
            return {}

    def _get_output_for_transactions(self, transaction_input, input_transactions_map):
        spent_transaction_hash = transaction_input.spent_transaction_hash
        input_transaction = input_transactions_map.get(spent_transaction_hash)
        if input_transaction is None:
            raise ValueError('Input transaction with hash {} not found'.format(spent_transaction_hash))

        spent_output_index = transaction_input.spent_output_index
        if input_transaction.outputs is None or len(input_transaction.outputs) < (spent_output_index + 1):
            raise ValueError(
                'There is no output with index {} in transaction with hash {}'.format(
                    spent_output_index, spent_transaction_hash))

        output = input_transaction.outputs[spent_output_index]
        return output

    def _end(self):
        self.batch_work_executor.shutdown()
        self.item_exporter.close()
