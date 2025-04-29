import json
import os
import logging
from dotenv import load_dotenv
from src.models.gemini_model import GeminiDialogueAnalyzer
from src.models.deepseek_model import DeepSeekDialogueAnalyzer
from src.utils.feishu_client import fetch_bitable_records, write_records_to_bitable, get_write_token
import math
import concurrent.futures
from functools import partial
# 移除 csv 和 Path 的导入
# import csv
# from pathlib import Path
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- 定义需要转换为数字的字段 ---
# 根据 system_prompt.txt 中的 "数字" 标记
NUMERIC_FIELDS = ["编号", "对话编号"]

# --- 定义输出文件名 ---
OUTPUT_FILENAME = "output.txt"

# --- 修改：用于并行执行的辅助函数，增加写入本地文件的逻辑 ---
def analyze_and_write_batch(
    batch_records_input: list, # 输入的原始飞书记录
    batch_num: int,
    total_batches: int,
    analyzer,
    feishu_write_app_token: str, # 目标 Bitable App Token
    feishu_write_table_id: str,  # 目标 Bitable Table ID
    write_token: str # 用于写入的认证 Token
):
    """
    分析单个批次的记录，并将结果写入飞书多维表格。
    Args:
        batch_records_input: 当前批次的原始飞书记录列表。
        batch_num: 当前批次的编号 (从 1 开始)。
        total_batches: 总批次数。
        analyzer: 模型分析器实例。
        feishu_write_app_token: 目标 Bitable App Token.
        feishu_write_table_id: 目标 Bitable Table ID.
        write_token: 用于写入的认证 Token.
    Returns:
        bool: 当前批次处理和写入是否成功。
    """
    logging.info(f"--- Processing Batch {batch_num}/{total_batches} ---")
    # 将原始记录转换为 JSON 字符串以传递给 LLM
    user_prompt_data = json.dumps(batch_records_input, ensure_ascii=False, indent=2)
    batch_success = False # 标记当前批次是否成功

    try:
        # 调用 LLM 分析
        result = analyzer.analyze_dialogue(user_prompt_content=user_prompt_data)
        logging.info(f"--- LLM Analysis Finished for Batch {batch_num}/{total_batches} ---")

        # --- 修改：增加对 LLM 结果有效性的检查 ---
        # 检查 LLM 是否返回了错误标记，或者结果不是预期的列表或字典
        is_llm_error = False
        if isinstance(result, list) and result and isinstance(result[0], dict) and "error" in result[0]:
            is_llm_error = True
            logging.error(f"LLM analysis error for batch {batch_num}: {result[0].get('error', 'Unknown LLM error')}")
            # 可以选择记录 raw_response
        elif not isinstance(result, (list, dict)): # 如果返回的不是列表也不是字典
             is_llm_error = True
             logging.error(f"LLM returned an unexpected data type for batch {batch_num}: {type(result)}")

        if is_llm_error:
            # LLM 分析失败，不尝试写入飞书，直接标记批次失败
            logging.warning(f"Skipping Feishu write for batch {batch_num} due to LLM analysis error.")
        # --- 结束检查 ---

        # --- 只有在 LLM 没有返回错误时才继续处理和写入 ---
        elif isinstance(result, list) and result: # 假设成功的 result 是一个评估项列表 (list of dicts)
            logging.info(f"LLM returned {len(result)} records for batch {batch_num}. Preparing to write...")

            # --- 数据类型转换 ---
            processed_records_for_feishu = []
            for record in result:
                if not isinstance(record, dict):
                    logging.warning(f"Skipping non-dict item in LLM result for batch {batch_num}: {record}")
                    continue

                processed_record = record.copy() # 创建副本以修改
                for field in NUMERIC_FIELDS:
                    if field in processed_record:
                        original_value = processed_record[field]
                        try:
                            # 尝试转换为整数
                            processed_record[field] = int(original_value)
                        except (ValueError, TypeError) as e:
                            logging.warning(f"Batch {batch_num}: Could not convert field '{field}' value '{original_value}' to int. Keeping original. Error: {e}")
                            # 保留原始值或设置为 None/0，取决于你的需求

                processed_records_for_feishu.append(processed_record)
            # --- 结束数据类型转换 ---

            if processed_records_for_feishu:
                # --- 新增：写入本地文件 ---
                try:
                    with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as f:
                        for record_to_write in processed_records_for_feishu:
                            # 将每个记录字典转换为 JSON 字符串并写入，每个记录占一行
                            f.write(json.dumps(record_to_write, ensure_ascii=False) + '\n')
                    logging.info(f"Successfully appended batch {batch_num} results to {OUTPUT_FILENAME}")
                except Exception as file_write_error:
                    logging.error(f"Failed to write batch {batch_num} results to {OUTPUT_FILENAME}: {file_write_error}")
                # --- 结束新增 ---

                # 调用写入飞书的函数
                write_success = write_records_to_bitable(
                    app_token=feishu_write_app_token,
                    table_id=feishu_write_table_id,
                    records=processed_records_for_feishu,
                    bearer_token=write_token
                )
                if write_success:
                    logging.info(f"Successfully wrote batch {batch_num} results to Feishu.")
                    batch_success = True
                else:
                    logging.error(f"Failed to write batch {batch_num} results to Feishu.")
            else:
                 logging.warning(f"Batch {batch_num}: No valid records to write after processing LLM results.")
                 batch_success = True # 这种情况也算处理完成

        elif isinstance(result, dict): # 如果结果是单个字典
             logging.info(f"LLM returned a single dict for batch {batch_num}. Preparing to write...")
             processed_record = result.copy()
             for field in NUMERIC_FIELDS:
                 if field in processed_record:
                     original_value = processed_record[field]
                     try:
                         processed_record[field] = int(original_value)
                     except (ValueError, TypeError) as e:
                         logging.warning(f"Batch {batch_num}: Could not convert field '{field}' value '{original_value}' to int. Keeping original. Error: {e}")

             # --- 新增：写入本地文件 ---
             try:
                 with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as f:
                     # 将单个记录字典转换为 JSON 字符串并写入
                     f.write(json.dumps(processed_record, ensure_ascii=False) + '\n')
                 logging.info(f"Successfully appended batch {batch_num} single result to {OUTPUT_FILENAME}")
             except Exception as file_write_error:
                 logging.error(f"Failed to write batch {batch_num} single result to {OUTPUT_FILENAME}: {file_write_error}")
             # --- 结束新增 ---

             # 调用写入飞书的函数
             write_success = write_records_to_bitable(
                 app_token=feishu_write_app_token,
                 table_id=feishu_write_table_id,
                 records=[processed_record], # 单个记录也用列表包装
                 bearer_token=write_token
             )
             if write_success:
                 logging.info(f"Successfully wrote batch {batch_num} single result to Feishu.")
                 batch_success = True
             else:
                 logging.error(f"Failed to write batch {batch_num} single result to Feishu.")

        else: # 处理空列表或其他意外情况 (非错误，但无数据)
            logging.warning(f"Batch {batch_num} returned an empty list from LLM. Nothing to write.")
            batch_success = True # 没有结果也算处理完成
        # --- 结束写入逻辑 ---

    except Exception as batch_error:
        logging.error(f"An unexpected error occurred during processing/writing of batch {batch_num}: {batch_error}")
        import traceback
        traceback.print_exc()
        # 异常发生，标记批次失败

    return batch_success


def main():
    try:
        logging.info("Starting the program...")
        logging.info("Loading environment variables...")
        load_dotenv()

        # --- 获取模型配置 ---
        google_api_key = os.getenv("GOOGLE_API_KEY")
        model_name = os.getenv("MODEL_NAME", "gemini-pro")
        temperature = float(os.getenv("TEMPERATURE", 0))
        max_output_tokens = int(os.getenv("MAX_OUTPUT_TOKENS", 2048))
        deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
        deepseek_base_url = os.getenv("DEEPSEEK_BASE_URL")
        deepseek_model_name = os.getenv("DEEPSEEK_MODEL_NAME")
        model_provider = os.getenv("MODEL_PROVIDER", "gemini").lower() # 默认为 gemini

        # --- 获取飞书读取配置 ---
        feishu_read_app_token = os.getenv("FEISHU_READ_APP_TOKEN")
        feishu_read_table_id = os.getenv("FEISHU_READ_TABLE_ID")
        feishu_read_view_id = os.getenv("FEISHU_READ_VIEW_ID")
        # <--- 移除读取字段配置

        # --- 获取飞书书写写入配置 (假设写入同一个表格) ---
        feishu_write_app_token = feishu_read_app_token # 使用与读取相同的 App Token
        feishu_write_table_id = feishu_read_table_id   # 使用与读取相同的 Table ID

        # --- 检查配置 ---
        if model_provider == "gemini" and not google_api_key:
             raise ValueError("GOOGLE_API_KEY is not set in the environment variables for Gemini.")
        if model_provider == "deepseek" and not deepseek_api_key:
             raise ValueError("DEEPSEEK_API_KEY is not set in the environment variables for DeepSeek.")
        if not feishu_read_app_token or not feishu_read_table_id:
            raise ValueError("FEISHU_APP_TOKEN or FEISHU_TABLE_ID for reading is not set.")
        if not feishu_write_app_token or not feishu_write_table_id:
            raise ValueError("FEISHU_APP_TOKEN or FEISHU_TABLE_ID for writing is not set.")


        # --- 初始化模型分析器 ---
        analyzer = None
        system_prompt_path = "src/prompts/system_prompt.txt"
        if not os.path.exists(system_prompt_path):
             raise FileNotFoundError(f"System prompt file not found at: {system_prompt_path}")
        with open(system_prompt_path, 'r', encoding='utf-8') as f:
            system_prompt = f.read()

        if model_provider == "gemini":
            logging.info("Initializing GeminiDialogueAnalyzer...")
            # --- 确保传递所有必需的参数 ---
            analyzer = GeminiDialogueAnalyzer(
                api_key=google_api_key,
                model_name=model_name, # 确保 model_name 已从 env 加载
                system_prompt=system_prompt,
                temperature=temperature, # 确保 temperature 已从 env 加载
                max_output_tokens=max_output_tokens # 确保 max_output_tokens 已从 env 加载
            )
        elif model_provider == "deepseek":
            logging.info("Initializing DeepSeekDialogueAnalyzer...")
            analyzer = DeepSeekDialogueAnalyzer(api_key=deepseek_api_key, base_url=deepseek_base_url,
                                                model_name=deepseek_model_name, system_prompt=system_prompt,
                                                temperature=temperature, max_output_tokens=max_output_tokens)
        else:
            raise ValueError(f"Unsupported model provider: {model_provider}. Choose 'gemini' or 'deepseek'.")

        if analyzer:
            # --- 获取飞书数据 ---
            logging.info(f"Fetching records (编号, round5, round10) from Feishu Bitable (Read): App={feishu_read_app_token}, Table={feishu_read_table_id}, View={feishu_read_view_id}")
            # --- 修改：移除 fields 参数传递 ---
            records = fetch_bitable_records(
                app_token=feishu_read_app_token,
                table_id=feishu_read_table_id,
                view_id=feishu_read_view_id
            )

            if not records:
                logging.warning("No records fetched from Feishu Bitable.")
                return # 没有数据则退出

            logging.info(f"Successfully fetched {len(records)} records from Feishu.")

            # --- 获取用于写入的飞书 Token ---
            feishu_writer_token = get_write_token()
            if not feishu_writer_token:
                logging.error("Could not obtain Feishu token for writing. Aborting.")
                return # 没有写入 token 则退出

            # --- 修改：并行处理记录并写入飞书 ---
            batch_size = 2 # 可以调整批处理大小
            num_batches = math.ceil(len(records) / batch_size)

           

            logging.info(f"Starting parallel dialogue analysis for {num_batches} batches of size {batch_size}...")

            successful_batches = 0
            failed_batches = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor: # 可调整并发数
                futures = []
                for i in range(0, len(records), batch_size):
                    batch_records_slice = records[i:i + batch_size]
                    current_batch_num = (i // batch_size) + 1
                    # 修改这里：调用新的函数并传递写入所需参数
                    task = partial(
                        analyze_and_write_batch,
                        batch_records_slice,
                        current_batch_num,
                        num_batches,
                        analyzer,
                        feishu_write_app_token, # 传递写入 App Token
                        feishu_write_table_id,  # 传递写入 Table ID
                        feishu_writer_token     # 传递写入 Token
                    )
                    futures.append(executor.submit(task))

                # 等待所有任务完成并统计结果
                for future in concurrent.futures.as_completed(futures):
                    try:
                        # 获取任务的返回结果（True 表示成功，False 表示失败）
                        was_successful = future.result()
                        if was_successful:
                            successful_batches += 1
                        else:
                            failed_batches += 1
                    except Exception as exc:
                        # 这个异常通常是 analyze_and_write_batch 内部未捕获的错误
                        logging.error(f'Batch analysis/writing generated an exception at future level: {exc}')
                        failed_batches += 1 # 将执行器级别的异常也算作失败

            # --- 结束并行处理修改 ---

            logging.info(f"All batches processed. Successful batches: {successful_batches}, Failed batches: {failed_batches}.")
            

            logging.info("Program finished successfully.")
        else:
             logging.error("Analyzer could not be initialized.")

    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {e}")
    except ValueError as e:
        logging.error(f"Configuration error: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
