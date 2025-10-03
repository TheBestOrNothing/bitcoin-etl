# run_all_${YM}.sh
set -e
./01_tag_block.sh
./02_tag_transaction.sh
./03_tag_output.sh
./04_tag_address.sh
./05_edge_belongs_to.sh
./06_edge_lock_to.sh
./07_edge_pay_to.sh
./08_edge_spent_by.sh
./09_edge_coinbase.sh
./10_edge_chain_to.sh
echo "All done for YM=${YM}"

