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


from typing import List, Optional
from datetime import datetime


class BtcTransactionInputOutput(object):
    def __init__(self):
        # ---- Inputs fields ----
        self.i_transaction_hash: Optional[str] = None
        self.i_input_index: Optional[int] = None
        self.i_block_hash: Optional[str] = None
        self.i_block_number: Optional[int] = None
        self.i_block_timestamp: Optional[datetime] = None
        self.i_spending_transaction_hash: Optional[str] = None
        self.i_spending_output_index: Optional[int] = None
        self.i_script_asm: Optional[str] = None
        self.i_script_hex: Optional[str] = None
        self.i_sequence: Optional[int] = None
        self.i_required_signatures: Optional[int] = None
        self.i_type: Optional[str] = None
        self.i_addresses: List[str] = []
        self.i_value: Optional[float] = None

        # ---- Outputs fields ----
        self.o_transaction_hash: Optional[str] = None
        self.o_output_index: Optional[int] = None
        self.o_block_hash: Optional[str] = None
        self.o_block_number: Optional[int] = None
        self.o_block_timestamp: Optional[datetime] = None
        self.o_spent_transaction_hash: Optional[str] = None
        self.o_spent_input_index: Optional[int] = None
        self.o_spent_block_hash: Optional[str] = None
        self.o_spent_block_number: Optional[int] = None
        self.o_spent_block_timestamp: Optional[datetime] = None
        self.o_script_asm: Optional[str] = None
        self.o_script_hex: Optional[str] = None
        self.o_required_signatures: Optional[int] = None
        self.o_type: Optional[str] = None
        self.o_addresses: List[str] = []
        self.o_value: Optional[float] = None
        self.o_is_coinbase: bool = False

        # ---- Revision ----
        self.revision: Optional[int] = None

    # ---- Helper methods (Optional) ----
    def total_value(self) -> float:
        """Return combined input + output value (can be useful for analytics)."""
        return (self.i_value or 0) + (self.o_value or 0)