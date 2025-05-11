import json
import os
import logging
import asyncio
from dotenv import load_dotenv
from src.models.gemini_model import GeminiDialogueAnalyzer
from src.models.deepseek_model import DeepSeekDialogueAnalyzer
# 导入MCP客户端替代原有feishu_client
from src.utils.feishu_mcp_client import FeishuMCPClient
import math
import concurrent.futures
from functools import partial

load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- 定义需要转换为数字的字段 ---
NUMERIC_FIELDS = ["编号", "对话编号"]

# --- 定义输出文件名 ---
OUTPUT_FILENAME = "output.txt"

# --- 创建MCP客户端实例 ---
feishu_mcp_client = FeishuMCPClient()

# --- 使用MCP客户端获取飞书数据 ---
async def fetch_bitable_records_mcp(app_token: str, table_id: str, view_id: str):
    """使用MCP客户端从飞书多维表格获取记录"""
    logging.info(f"使用MCP客户端从飞书获取记录: App={app_token}, Table={table_id}, View={view_id}")
    
    result = await feishu_mcp_client.fetch_records(app_token, table_id, view_id)
    
    if result["success"]:
        logging.info(f"成功获取 {len(result['records'])} 条记录")
        return result["records"]
    else:
        logging.error(f"获取记录失败: {result.get('error', '未知错误')}")
        return []

# --- 使用MCP客户端写入飞书数据 ---
async def write_records_to_bitable_mcp(app_token: str, table_id: str, records: list, bearer_token: str = None):
    """使用MCP客户端将记录写入到飞书多维表格"""
    logging.info(f"使用MCP客户端写入 {len(records)} 条记录到飞书: App={app_token}, Table={table_id}")
    
    result = await feishu_mcp_client.write_records(app_token, table_id, records, bearer_token)
    
    if result["success"]:
        logging.info(f"成功写入记录到飞书: {result.get('message', '')}")
        return True
    else:
        logging.error(f"写入记录失败: {result.get('error', '未知错误')}")
        return False

# --- 使用MCP客户端获取Token ---
async def get_write_token_mcp():
    """使用MCP客户端获取用于写入的Token"""
    logging.info("使用MCP客户端获取飞书写入Token")
    
    # 获取App ID和Secret
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    
    if not app_id or not app_secret:
        logging.warning("未找到FEISHU_APP_ID或FEISHU_APP_SECRET，尝试使用环境变量中的Bearer Token")
        return os.getenv("FEISHU_BEARER_TOKEN")
    
    # 调用get_tenant_access_token获取token
    result = await feishu_mcp_client.get_server_info()
    if "error" in result:
        logging.error(f"获取服务器信息失败: {result.get('error')}")
        return os.getenv("FEISHU_BEARER_TOKEN")
    
    # 我们需要通过先尝试调用任一MCP工具函数来获取token
    # 由于MCP服务器会自动处理token获取，因此我们只需要一个小的测试调用
    test_app_token = os.getenv("FEISHU_READ_APP_TOKEN", "test")
    test_table_id = os.getenv("FEISHU_READ_TABLE_ID", "test")
    test_result = await feishu_mcp_client.fetch_records(
        app_token=test_app_token,
        table_id=test_table_id,
        view_id="test",
        bearer_token=None  # 不传token，让服务器端自动获取
    )
    
    # 这里我们无法直接获取token，只能假设token已经被服务器端获取
    # 实际写入时将传递None让服务器端处理
    return None

# --- 修改：使用MCP客户端分析和写入批次 ---
async def analyze_and_write_batch_mcp(
    batch_records_input: list,
    batch_num: int,
    total_batches: int,
    analyzer,
    feishu_write_app_token: str,
    feishu_write_table_id: str,
    write_token = None
):
    """
    使用MCP客户端分析单个批次的记录，并将结果写入飞书多维表格。
    Args:
        batch_records_input: 当前批次的原始飞书记录列表。
        batch_num: 当前批次的编号 (从 1 开始)。
        total_batches: 总批次数。
        analyzer: 模型分析器实例。
        feishu_write_app_token: 目标 Bitable App Token.
        feishu_write_table_id: 目标 Bitable Table ID.
        write_token: 用于写入的认证Token，可为None (MCP服务器将自动处理)。
    Returns:
        bool: 当前批次处理和写入是否成功。
    """
    logging.info(f"--- 使用MCP客户端处理批次 {batch_num}/{total_batches} ---")
    # 将原始记录转换为 JSON 字符串以传递给 LLM
    user_prompt_data = json.dumps(batch_records_input, ensure_ascii=False, indent=2)
    batch_success = False # 标记当前批次是否成功

    try:
        # 调用 LLM 分析
        result = analyzer.analyze_dialogue(user_prompt_content=user_prompt_data)
        logging.info(f"--- LLM 分析完成，批次 {batch_num}/{total_batches} ---")

        # --- 增加对 LLM 结果有效性的检查 ---
        is_llm_error = False
        if isinstance(result, list) and result and isinstance(result[0], dict) and "error" in result[0]:
            is_llm_error = True
            logging.error(f"批次 {batch_num} 的 LLM 分析错误: {result[0].get('error', '未知 LLM 错误')}")
        elif not isinstance(result, (list, dict)):
            is_llm_error = True
            logging.error(f"批次 {batch_num} 中 LLM 返回了意外的数据类型: {type(result)}")

        if is_llm_error:
            # LLM 分析失败，不尝试写入飞书，直接标记批次失败
            logging.warning(f"由于 LLM 分析错误，跳过批次 {batch_num} 的飞书写入")
            return False

        # --- 只有在 LLM 没有返回错误时才继续处理和写入 ---
        if isinstance(result, list) and result:
            logging.info(f"批次 {batch_num} 中 LLM 返回了 {len(result)} 条记录。准备写入...")

            # --- 数据类型转换 ---
            processed_records_for_feishu = []
            for record in result:
                if not isinstance(record, dict):
                    logging.warning(f"跳过批次 {batch_num} 中非字典类型的 LLM 结果: {record}")
                    continue

                processed_record = record.copy() # 创建副本以修改
                for field in NUMERIC_FIELDS:
                    if field in processed_record:
                        original_value = processed_record[field]
                        try:
                            # 尝试转换为整数
                            processed_record[field] = int(original_value)
                        except (ValueError, TypeError) as e:
                            logging.warning(f"批次 {batch_num}: 无法将字段 '{field}' 值 '{original_value}' 转换为整数。保留原始值。错误: {e}")

                processed_records_for_feishu.append(processed_record)
            # --- 结束数据类型转换 ---

            if processed_records_for_feishu:
                # --- 写入本地文件 ---
                try:
                    with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as f:
                        for record_to_write in processed_records_for_feishu:
                            # 将每个记录字典转换为 JSON 字符串并写入，每个记录占一行
                            f.write(json.dumps(record_to_write, ensure_ascii=False) + '\n')
                    logging.info(f"成功将批次 {batch_num} 结果追加到 {OUTPUT_FILENAME}")
                except Exception as file_write_error:
                    logging.error(f"无法将批次 {batch_num} 结果写入 {OUTPUT_FILENAME}: {file_write_error}")

                # 使用MCP客户端调用写入飞书的函数
                write_success = await write_records_to_bitable_mcp(
                    app_token=feishu_write_app_token,
                    table_id=feishu_write_table_id,
                    records=processed_records_for_feishu,
                    bearer_token=write_token
                )
                
                if write_success:
                    logging.info(f"成功写入批次 {batch_num} 结果到飞书")
                    batch_success = True
                else:
                    logging.error(f"无法写入批次 {batch_num} 结果到飞书")
            else:
                logging.warning(f"批次 {batch_num}: 处理 LLM 结果后没有有效记录可写入")
                batch_success = True # 这种情况也算处理完成

        elif isinstance(result, dict):
            logging.info(f"批次 {batch_num} 中 LLM 返回了单个字典。准备写入...")
            processed_record = result.copy()
            for field in NUMERIC_FIELDS:
                if field in processed_record:
                    original_value = processed_record[field]
                    try:
                        processed_record[field] = int(original_value)
                    except (ValueError, TypeError) as e:
                        logging.warning(f"批次 {batch_num}: 无法将字段 '{field}' 值 '{original_value}' 转换为整数。保留原始值。错误: {e}")

            # --- 写入本地文件 ---
            try:
                with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as f:
                    # 将单个记录字典转换为 JSON 字符串并写入
                    f.write(json.dumps(processed_record, ensure_ascii=False) + '\n')
                logging.info(f"成功将批次 {batch_num} 单个结果追加到 {OUTPUT_FILENAME}")
            except Exception as file_write_error:
                logging.error(f"无法将批次 {batch_num} 单个结果写入 {OUTPUT_FILENAME}: {file_write_error}")

            # 使用MCP客户端调用写入飞书的函数
            write_success = await write_records_to_bitable_mcp(
                app_token=feishu_write_app_token,
                table_id=feishu_write_table_id,
                records=[processed_record],
                bearer_token=write_token
            )
            
            if write_success:
                logging.info(f"成功将批次 {batch_num} 单个结果写入飞书")
                batch_success = True
            else:
                logging.error(f"无法将批次 {batch_num} 单个结果写入飞书")

        else: # 处理空列表或其他意外情况 (非错误，但无数据)
            logging.warning(f"批次 {batch_num} 从 LLM 返回了空列表。没有内容可写入")
            batch_success = True # 没有结果也算处理完成

    except Exception as batch_error:
        logging.error(f"在处理/写入批次 {batch_num} 期间发生意外错误: {batch_error}")
        import traceback
        traceback.print_exc()

    return batch_success

# 使用ThreadPoolExecutor运行异步任务的辅助函数
def run_async_in_thread(coro):
    """在线程中运行异步协程并返回结果"""
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# 修改原有analyze_and_write_batch函数，将其封装为同步版本以兼容ThreadPoolExecutor
def analyze_and_write_batch(
    batch_records_input: list,
    batch_num: int,
    total_batches: int,
    analyzer,
    feishu_write_app_token: str,
    feishu_write_table_id: str,
    write_token = None
):
    """
    同步版本的批次处理函数，内部调用异步版本。
    """
    return run_async_in_thread(
        analyze_and_write_batch_mcp(
            batch_records_input,
            batch_num,
            total_batches,
            analyzer,
            feishu_write_app_token,
            feishu_write_table_id,
            write_token
        )
    )

async def main_async():
    """主函数的异步版本"""
    try:
        logging.info("启动程序...")
        logging.info("加载环境变量...")
        load_dotenv()

        # --- 程序启动时清空 output.txt ---
        try:
            with open(OUTPUT_FILENAME, 'w', encoding='utf-8') as f:
                # 打开并立即关闭，达到清空目的
                pass
            logging.info(f"清空之前的 {OUTPUT_FILENAME} 内容")
        except Exception as e:
            logging.error(f"无法清空 {OUTPUT_FILENAME}: {e}")

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

        # --- 获取飞书写入配置 (假设写入同一个表格) ---
        feishu_write_app_token = feishu_read_app_token # 使用与读取相同的 App Token
        feishu_write_table_id = feishu_read_table_id   # 使用与读取相同的 Table ID

        # --- 检查配置 ---
        if model_provider == "gemini" and not google_api_key:
             raise ValueError("环境变量中未设置 GOOGLE_API_KEY (用于 Gemini)")
        if model_provider == "deepseek" and not deepseek_api_key:
             raise ValueError("环境变量中未设置 DEEPSEEK_API_KEY (用于 DeepSeek)")
        if not feishu_read_app_token or not feishu_read_table_id:
            raise ValueError("环境变量中未设置读取用的 FEISHU_APP_TOKEN 或 FEISHU_TABLE_ID")
        if not feishu_write_app_token or not feishu_write_table_id:
            raise ValueError("环境变量中未设置写入用的 FEISHU_APP_TOKEN 或 FEISHU_TABLE_ID")

        # --- 初始化模型分析器 ---
        analyzer = None
        system_prompt_path = "src/prompts/system_prompt.txt"
        if not os.path.exists(system_prompt_path):
             raise FileNotFoundError(f"系统提示文件未找到: {system_prompt_path}")
        with open(system_prompt_path, 'r', encoding='utf-8') as f:
            system_prompt = f.read()

        if model_provider == "gemini":
            logging.info("初始化 GeminiDialogueAnalyzer...")
            analyzer = GeminiDialogueAnalyzer(
                api_key=google_api_key,
                model_name=model_name,
                system_prompt=system_prompt,
                temperature=temperature,
                max_output_tokens=max_output_tokens
            )
        elif model_provider == "deepseek":
            logging.info("初始化 DeepSeekDialogueAnalyzer...")
            analyzer = DeepSeekDialogueAnalyzer(api_key=deepseek_api_key, base_url=deepseek_base_url,
                                                model_name=deepseek_model_name, system_prompt=system_prompt,
                                                temperature=temperature, max_output_tokens=max_output_tokens)
        else:
            raise ValueError(f"不支持的模型提供商: {model_provider}。选择 'gemini' 或 'deepseek'。")

        if analyzer:
            # --- 使用MCP客户端获取飞书数据 ---
            logging.info(f"使用MCP客户端从飞书获取记录: App={feishu_read_app_token}, Table={feishu_read_table_id}, View={feishu_read_view_id}")
            records = await fetch_bitable_records_mcp(
                app_token=feishu_read_app_token,
                table_id=feishu_read_table_id,
                view_id=feishu_read_view_id
            )

            if not records:
                logging.warning("从飞书多维表格获取的记录为空")
                return # 没有数据则退出

            logging.info(f"成功从飞书获取 {len(records)} 条记录")

            # --- 打印和检查获取的记录 ---
            logging.info("--- 打印前几条从飞书获取的记录 ---")
            for i, record in enumerate(records[:5]):
                logging.info(f"记录 {i+1} 原始数据: {record}")
                if "编号" in record:
                    logging.info(f"  记录 {i+1}: '编号' 字段已找到。值: {record.get('编号')}")
                else:
                    logging.warning(f"  记录 {i+1}: '编号' 字段未在键中找到: {list(record.keys())}")
            logging.info("--- 打印记录完成 ---")

            # --- 根据【编号】字段去重 ---
            unique_records = []
            seen_ids = set()

            for record in records:
                record_id_value = record.get("编号")
                
                if record_id_value is not None and record_id_value not in seen_ids:
                    unique_records.append(record)
                    seen_ids.add(record_id_value)
                elif record_id_value is None:
                    partial_record_info = str(list(record.items())[:2])
                    logging.warning(f"记录 (fields: {partial_record_info}...) 因缺少 '编号' 字段而被跳过")
                else:
                    partial_record_info = str(list(record.items())[:2])
                    logging.debug(f"记录 (fields: {partial_record_info}...) 因 '编号' 重复而被跳过: {record_id_value}")

            original_count = len(records)
            records = unique_records
            logging.info(f"根据 '编号' 字段对记录进行去重。原始: {original_count}, 唯一: {len(records)}")

            # --- 将去重后的记录追加写入 output.txt ---
            if records:
                try:
                    with open(OUTPUT_FILENAME, 'a', encoding='utf-8') as f:
                        f.write("--- 去重后的飞书记录 ---\n")
                        for record_to_write in records:
                            f.write(json.dumps(record_to_write, ensure_ascii=False) + '\n')
                    logging.info(f"成功将 {len(records)} 条去重后的记录追加到 {OUTPUT_FILENAME}")
                except Exception as file_write_error:
                    logging.error(f"无法将去重后的记录追加到 {OUTPUT_FILENAME}: {file_write_error}")

            if not records:
                logging.warning("去重后没有记录剩余或写入失败")
                return

            # --- 获取用于写入的飞书 Token ---
            logging.info("获取用于写入的飞书 Token...")
            feishu_writer_token = await get_write_token_mcp()

            # --- 并行处理记录并写入飞书 ---
            batch_size = 2 # 可以调整批处理大小
            num_batches = math.ceil(len(records) / batch_size)

            logging.info(f"开始并行对话分析，共 {num_batches} 个批次，每批 {batch_size} 条记录...")

            successful_batches = 0
            failed_batches = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for i in range(0, len(records), batch_size):
                    batch_records_slice = records[i:i + batch_size]
                    current_batch_num = (i // batch_size) + 1
                    task = partial(
                        analyze_and_write_batch,
                        batch_records_slice,
                        current_batch_num,
                        num_batches,
                        analyzer,
                        feishu_write_app_token,
                        feishu_write_table_id,
                        feishu_writer_token
                    )
                    futures.append(executor.submit(task))

                # 等待所有任务完成并统计结果
                for future in concurrent.futures.as_completed(futures):
                    try:
                        was_successful = future.result()
                        if was_successful:
                            successful_batches += 1
                        else:
                            failed_batches += 1
                    except Exception as exc:
                        logging.error(f'批次分析/写入在 future 级别生成异常: {exc}')
                        failed_batches += 1

            logging.info(f"所有批次处理完成。成功批次: {successful_batches}, 失败批次: {failed_batches}")
            logging.info("程序成功完成")
        else:
             logging.error("无法初始化分析器")

    except FileNotFoundError as e:
        logging.error(f"配置文件未找到: {e}")
    except ValueError as e:
        logging.error(f"配置错误: {e}")
    except Exception as e:
        logging.error(f"main 中发生意外错误: {e}")
        import traceback
        traceback.print_exc()

def main():
    """同步主函数，启动异步主函数"""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()