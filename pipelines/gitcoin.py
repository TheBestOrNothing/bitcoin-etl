from typing import List, Union, Generator, Iterator
from schemas import OpenAIChatMessage
from pydantic import BaseModel
import os
import requests
from typing import Any
from vanna.deepseek import DeepSeekChat
from vanna.chromadb import ChromaDB_VectorStore
from pandas import DataFrame
import base64
import json

class Pipeline:

    class DeepSeekVanna(ChromaDB_VectorStore, DeepSeekChat):
        def __init__(self, config=None):
            ChromaDB_VectorStore.__init__(self, config=config)
            DeepSeekChat.__init__(self, config=config)

        def generate_sql(self, question: str, cust_prompt : str,allow_llm_to_see_data=False, **kwargs) -> str:
            print("===============================================generate_sql=============================================")
            if self.config is not None:
                initial_prompt = self.config.get("initial_prompt", None)
            else:
                initial_prompt = None
            question_sql_list = self.get_similar_question_sql(question, **kwargs)
            ddl_list = self.get_related_ddl(question, **kwargs)
            doc_list = self.get_related_documentation(question, **kwargs)
            prompt = self.get_sql_prompt(
                initial_prompt=initial_prompt,
                cust_prompt=cust_prompt,
                question=question,
                question_sql_list=question_sql_list,
                ddl_list=ddl_list,
                doc_list=doc_list,
                **kwargs,
            )
            self.log(title="SQL Prompt", message=prompt)
            llm_response = self.submit_prompt(prompt, **kwargs)
            self.log(title="LLM Response", message=llm_response)

            if 'intermediate_sql' in llm_response:
                if not allow_llm_to_see_data:
                    return "The LLM is not allowed to see the data in your database. Your question requires database introspection to generate the necessary SQL. Please set allow_llm_to_see_data=True to enable this."

                if allow_llm_to_see_data:
                    intermediate_sql = self.extract_sql(llm_response)

                    try:
                        self.log(title="Running Intermediate SQL", message=intermediate_sql)
                        df = self.run_sql(intermediate_sql)

                        prompt = self.get_sql_prompt(
                            initial_prompt=initial_prompt,
                            question=question,
                            question_sql_list=question_sql_list,
                            ddl_list=ddl_list,
                            doc_list=doc_list + [
                                f"The following is a pandas DataFrame with the results of the intermediate SQL query {intermediate_sql}: \n" + df.to_markdown()],
                            **kwargs,
                        )
                        self.log(title="Final SQL Prompt", message=prompt)
                        llm_response = self.submit_prompt(prompt, **kwargs)
                        self.log(title="LLM Response", message=llm_response)
                    except Exception as e:
                        return f"Error running intermediate SQL: {e}"
            # return self.extract_sql(llm_response)
            print(f"===================>llm_response:{llm_response}")
            return llm_response
        
        def get_sql_prompt(
            self,
            initial_prompt : str,
            cust_prompt : str,
            question: str,
            question_sql_list: list,
            ddl_list: list,
            doc_list: list,
            **kwargs,
        ):
            print("===============================================get_sql_prompt=============================================")
            if initial_prompt is None:
                initial_prompt = f"You are a {self.dialect} expert. " + \
                "Please help to generate a SQL query to answer the question. Your response should ONLY be based on the given context and follow the response guidelines and format instructions. "

            initial_prompt = self.add_ddl_to_prompt(
                initial_prompt, ddl_list, max_tokens=self.max_tokens
            )

            if self.static_documentation != "":
                doc_list.append(self.static_documentation)

            initial_prompt = self.add_documentation_to_prompt(
                initial_prompt, doc_list, max_tokens=self.max_tokens
            )

            print(f"cust_prompt================>begin\n{cust_prompt}")
            print(f"cust_prompt================>end")

            initial_prompt += cust_prompt
            message_log = [self.system_message(initial_prompt)]

            for example in question_sql_list:
                if example is None:
                    print("example is None")
                else:
                    if example is not None and "question" in example and "sql" in example:
                        message_log.append(self.user_message(example["question"]))
                        message_log.append(self.assistant_message(example["sql"]))

            message_log.append(self.user_message(question))

            return message_log

    class Valves(BaseModel):
        VANNA_PROMPT: str = ""
        DEEPSEEK_API_KEY: str = ""
        pass

    def __init__(self):
        self.name = "Gitcoin"
        init_prompt=("===Response Guidelines \n"
            "1. If the provided context is sufficient, please first briefly rephrase or summarize the user's question in one sentence, then provide a valid SQL query for the question. \n"
            "2. If the provided context is almost sufficient but requires knowledge of a specific string in a particular column, please generate an intermediate SQL query to find the distinct strings in that column. Prepend the query with a comment saying intermediate_sql \n"
            "3. If the provided context is insufficient, please explain why it can't be generated. \n"
            "4. Please use the most relevant table(s). \n"
            "5. If the question has been asked and answered before, please repeat the answer exactly as it was given before. \n"
            "6. Ensure that the output SQL is PostgreSQL-compliant and executable, and free of syntax errors."
            """
            Output Format:

            "Question: {put your question here, as detailed as possible}"

            "-----"

            ```sql
            {put your SQL query here, without any comments or explanations}
            ```

            """
            )
        self.valves = self.Valves(
            **{
                "DEEPSEEK_API_KEY": os.getenv("DEEPSEEK_API_KEY", "sk-734feff8cffd415a92e11d725610e5d"),
                "VANNA_PROMPT": os.getenv("VANNA_PROMPT", init_prompt)
            }
        )
        self.vn = self.DeepSeekVanna(config={"api_key": self.valves.DEEPSEEK_API_KEY, "model": "deepseek-chat"})
        self.vn.connect_to_clickhouse(host='192.168.2.242', dbname='bitcoin', user='default', password='password', port=8123)
        
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

    def plot_figure_image(self, question: str, sql: str, df: DataFrame) -> str:
        print(f"plot_figure_image_202506")
        plotly_code = self.vn.generate_plotly_code(
            question=question,
            sql=sql,
            df_metadata=f"Running df.dtypes gives:\n {df.dtypes}",
        )
        print(f"plotly_code:{plotly_code}")

        fig = self.vn.get_plotly_figure(plotly_code=plotly_code, df=df)
        print(f"fig success:{fig}")

        img_data = fig.to_image(format="png", scale=1)
        print(f"img_data:{img_data}")

        img_base64 = base64.b64encode(img_data).decode('utf-8')
        print(f"img_base64={img_base64}")

        img_md = f"![figure](data:image/png;base64,{img_base64})"
        print(f"img_md={img_md}")

        return img_md
    
            
    def pipe(
            self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Generator[str | None, Any, None]:
        try:
            print(f"Question ==========================>: {user_message}")

            # Generate SQL phase
            try:
                answer = self.vn.generate_sql(question=user_message, cust_prompt=self.valves.VANNA_PROMPT, allow_llm_to_see_data="true")
            except Exception as e:
                error_msg = f"Error generating SQL: {str(e)}"
                print(f"SQL generation error: {error_msg}")
                yield f"\n{error_msg}\n"
                return

            yield f"{answer}\n"
            print(f"Generated Answer ==========================>: {answer}")

            # Execute SQL phase
            try:
                sql = self.vn.extract_sql(answer)
                df = self.vn.run_sql(sql)
            except Exception as e:
                error_msg = f"Error executing SQL: {str(e)}"
                print(f"SQL execution error: {error_msg}")
                yield f"\n{error_msg}\n"
                return

            # Handle empty results
            if df.empty:
                yield "\nQuery returned empty results\n"
                return

            # Data conversion phase
            try:
                dataframe = df.to_dict(orient="records")
            except Exception as e:
                error_msg = f"Data conversion failed: {str(e)}"
                print(f"Data conversion error: {error_msg}")
                yield f"\n{error_msg}\n"
                return

            # Generate table phase
            try:
                if len(dataframe) == 0:
                    yield "\nNo valid data to display\n"
                    return

                if len(dataframe) == 1:
                    compact_json = df.to_json(orient='records')
                    try:
                        parsed_data = json.loads(compact_json)
                        formatted_json = json.dumps(parsed_data, indent=4, ensure_ascii=False)
                        print(f"Dataframe parsing JSON success: {formatted_json}")

                        if formatted_json.startswith('[') and formatted_json.endswith(']'):
                            stripped_json_str = formatted_json[1:-1]
                        else:
                            stripped_json_str = formatted_json
                        markdown_response = f"```JSON\n{stripped_json_str}\n```"
                        yield f"\nGenerated Json:\n"
                        yield markdown_response
                    except Exception as e:
                        error_msg = f"Dataframe parsing JSON failed: {str(e)}"
                        print(f"Dataframe parsing JSON failed: {error_msg}")
                        yield f"\n{error_msg}\n"
                else:
                    headers = list(dataframe[0].keys())
                    mdtable = "| " + " | ".join(headers) + " |\n"
                    mdtable += "| " + " | ".join(["---"] * len(headers)) + " |\n"
                    for row in dataframe:
                        mdtable += "| " + " | ".join(str(row[h]) for h in headers) + " |\n"
                    yield f"\nGenerated DataFrame:\n"
                    yield f"\n{mdtable}\n"
            except (IndexError, KeyError) as e:
                error_msg = f"Table generation failed: Data format exception ({str(e)})"
                print(f"Table generation error: {error_msg}")
                yield f"\n{error_msg}\n"
            except Exception as e:
                error_msg = f"Unexpected error in table generation: {str(e)}"
                print(f"Table generation error: {error_msg}")
                yield f"\n{error_msg}\n"

            # Generate chart phase
            try:
                mdchart = self.plot_figure_image(
                    question=user_message,
                    sql=sql,
                    df=df
                )
                print(f"===========mdchart===========>{mdchart}")
                if mdchart is not None:
                    print(f"mdchart is not None")
                    yield f"\nGenerated Phase:\n"
                    yield f"\n{mdchart}\n"
                else:
                    print(f"mdchart is None")
                    yield None
            except Exception as e:
                error_msg = f"Chart generation failed: {str(e)}"
                print(f"Chart generation error: {error_msg}")
                yield f"\n{error_msg}\n"

        except Exception as e:
            error_msg = f"Unexpected error in processing pipeline: {str(e)}"
            print(f"Global error: {error_msg}")
            yield f"\n{error_msg}\n"


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

