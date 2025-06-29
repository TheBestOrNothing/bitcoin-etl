from typing import List, Union, Generator, Iterator
from schemas import OpenAIChatMessage
from pydantic import BaseModel
import os
import requests

from vanna.deepseek import DeepSeekChat
from vanna.chromadb import ChromaDB_VectorStore


class Pipeline:

    class DeepSeekVanna(ChromaDB_VectorStore, DeepSeekChat):
        def __init__(self, config=None):
            ChromaDB_VectorStore.__init__(self, config=config)
            DeepSeekChat.__init__(self, config=config)


    class Valves(BaseModel):
        DEEPSEEK_API_KEY: str = ""
        pass

    def __init__(self):
        # Optionally, you can set the id and name of the pipeline.
        # Best practice is to not specify the id so that it can be automatically inferred from the filename, so that users can install multiple versions of the same pipeline.
        # The identifier must be unique across all pipelines.
        # The identifier must be an alphanumeric string that can include underscores or hyphens. It cannot contain spaces, special characters, slashes, or backslashes.
        # self.id = "openai_pipeline"
        self.name = "Bitcoin"
        self.valves = self.Valves(
            **{
                "DEEPSEEK_API_KEY": os.getenv(
                    "DEEPSEEK_API_KEY", "sk-734feff8cffd415a92e11d725610e5d"
                )
            }
        )
        self.vn = self.DeepSeekVanna(config={"api_key": self.valves.DEEPSEEK_API_KEY, "model": "deepseek-chat"})
        # self.vn.connect_to_postgres(host='mypostgres.cql0keme6wne.us-east-1.rds.amazonaws.com', dbname='northwind', user='postgresAdmin', password='Zhe2p0stgres', port='5432')
        # self.vn.connect_to_postgres(host='host.docker.internal', dbname='northwind', user='postgres', password='postgres', port='55432')
        self.vn.connect_to_clickhouse(host='192.168.2.242', dbname='bitcoin', user='default', password='password', port=8123)
        # df_information_schema = self.vn.run_sql("SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, data_type, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema ='public'")
        # print(f"df_information_schema:{df_information_schema.to_string(index=False)}")
        # plan = self.vn.get_training_plan_generic(df_information_schema)
        # self.vn.train(plan=plan)
        # self.vn.train(ddl=NORTHWIND_DDL)
        
        traing_data_df = self.vn.get_training_data()
        if not traing_data_df.empty:
            print(f"Training data already exists, skipping training.")
            for _, row in traing_data_df.iterrows():
                traing_data_id = row['id']
                success = self.vn.remove_training_data(id = traing_data_id)
                if success:
                    print(f"Removed training data with id: {traing_data_id}")
                else:
                    print(f"Failed to remove training data with id: {traing_data_id}")
        else:
            print(f"No training data found, proceeding with training.")

        self.vn.train(ddl=BITCOIN_DDL)

        print(f"vanna ready:{__name__}")
        print(f"vanna ready:{self.valves.DEEPSEEK_API_KEY}")
        pass

    async def on_startup(self):
        # This function is called when the server is started.
        print(f"on_startup:{__name__}")
        pass

    async def on_shutdown(self):
        # This function is called when the server is stopped.
        print(f"on_shutdown:{__name__}")
        pass

    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        # This is where you can add your custom pipelines like RAG.
        print(f"pipe:{__name__}")

        print("messages:")
        print(messages)
        print("user_message:")
        print(user_message)

        # for chunk in ["Hello", "from", "the", "pipeline!"]:
        #    yield f"{chunk}\n\n"

        try:
            sql = self.vn.generate_sql(question=user_message)
            print(f"sql:{sql}")
            yield(f"sql:{sql}")
            df = self.vn.run_sql(sql)
            print(f"df:{df}")
            if df is not None:
                # Convert the DataFrame to a string representation
                df_str = df.to_string(index=False)
                # Return the DataFrame as a string
                yield f"SQL: {sql}\n\nDataFrame:\n{df_str}"
            else:
                yield "No data found."
        except Exception as e:
            yield f"Error: {e}"


BITCOIN_DDL = """

CREATE TABLE blocks_fat
(
  hash String,
  size UInt64,
  stripped_size UInt64,
  weight UInt64,
  number UInt64,
  version UInt64,
  merkle_root String,
  timestamp DateTime,
  timestamp_month Date,
  nonce String,
  bits String,
  coinbase_param String,
  previous_block_hash String,
  difficulty Float64,
  transaction_count UInt64,
  transactions Array(String)
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY (hash)
PARTITION BY toYYYYMM(timestamp_month)
ORDER BY hash;

CREATE TABLE transactions_fat
(
  hash String,
  size UInt64,
  virtual_size UInt64,
  version UInt64,
  lock_time UInt64,
  block_hash String,
  block_number UInt64,
  block_timestamp DateTime,
  block_timestamp_month Date,
  is_coinbase BOOL,
  input_count UInt64,
  output_count UInt64,
  input_value Float64,
  output_value Float64,
  fee Float64,
  inputs Array(Tuple(index UInt64, spent_transaction_hash String, spent_output_index UInt64, script_asm String, script_hex String, sequence UInt64, required_signatures UInt64, type String, addresses Array(String), value Float64)),
  outputs Array(Tuple(index UInt64, script_asm String, script_hex String, required_signatures UInt64, type String, addresses Array(String), value Float64))
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY (hash)
PARTITION BY toYYYYMM(block_timestamp_month)
ORDER BY (hash);

CREATE TABLE inputs
(
    transaction_hash String,
    input_index UInt64,
    block_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    spending_transaction_hash String,
    spending_output_index UInt64,
    script_asm String,
    script_hex String,
    sequence UInt64,
    required_signatures UInt64,
    type String,
    addresses Array(String),
    value Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (transaction_hash, input_index);

CREATE TABLE outputs
(
    transaction_hash String,
    output_index UInt64,
    block_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    spent_transaction_hash String,
    spent_input_index UInt64,
    script_asm String,
    script_hex String,
    required_signatures UInt64,
    type String,
    addresses Array(String),
    value Float64,
    version DateTime
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (transaction_hash, output_index);

CREATE TABLE address_flat
(
    transaction_hash String,
    output_index UInt64,
    block_hash String,
    block_number UInt64,
    block_timestamp DateTime,
    address String,
    value Float64,
    spent_transaction_hash String,
    spent_input_index UInt64,
    version DateTime
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(block_timestamp)
ORDER BY (address, transaction_hash, output_index);
"""