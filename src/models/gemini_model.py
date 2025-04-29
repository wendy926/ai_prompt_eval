import logging
import json
import re # <--- 添加 re
from typing import Optional, Dict, Any, List # <--- 修改 typing 导入
from langchain_google_genai import ChatGoogleGenerativeAI

# --- 添加 LangChain 消息类型 ---
from langchain.schema import SystemMessage, HumanMessage

class GeminiDialogueAnalyzer:
    """使用 Gemini 模型分析对话内容的类""" # <--- 更新文档字符串

    # --- 修改 __init__ 方法 ---
    def __init__(
        self,
        api_key: str,
        model_name: str, # <--- 添加 model_name
        system_prompt: str, # <--- 添加 system_prompt
        temperature: float = 0.7,
        max_output_tokens: int = 2048 # <--- 使用 max_output_tokens
    ):
        """
        初始化 GeminiDialogueAnalyzer。

        Args:
            api_key (str): Google API 密钥。
            model_name (str): 要使用的 Gemini 模型名称 (例如 'gemini-2.5-pro-latest')。
            system_prompt (str): 用于指导模型的系统提示。
            temperature (float): 控制生成文本的随机性。
            max_output_tokens (int): 生成响应的最大 token 数。
        """
        if not api_key:
            raise ValueError("Google API key is required.")
        if not model_name:
            raise ValueError("Gemini model name is required.")
        if not system_prompt:
             raise ValueError("System prompt is required.")

        self.api_key = api_key
        self.model_name = model_name
        self.system_prompt = system_prompt
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens

        try:
            # 初始化 LangChain 的 ChatGoogleGenerativeAI
            self.llm = ChatGoogleGenerativeAI(
                google_api_key=self.api_key,
                model=self.model_name, # <--- 使用传入的 model_name
                temperature=self.temperature, # <--- 使用传入的 temperature
                max_output_tokens=self.max_output_tokens # <--- 使用传入的 max_output_tokens
                # LangChain 的 ChatGoogleGenerativeAI 可能没有直接的 base_url 参数
            )
            logging.info(f"Gemini client initialized successfully for model: {self.model_name}")
        except Exception as e:
            logging.error(f"Failed to initialize Gemini client: {e}")
            raise ConnectionError(f"Failed to initialize Gemini client: {e}")
        
    # --- 结束 __init__ 修改 ---

    # --- 修改 analyze_dialogue 方法 ---
    def analyze_dialogue(self, user_prompt_content: str) -> List[Dict[str, Any]]: # <--- 修改返回类型
        """
        使用 Gemini 模型分析提供的对话内容。

        Args:
            user_prompt_content (str): 包含对话内容的 JSON 字符串 (飞书表格数据)。

        Returns:
            List[Dict[str, Any]]: 分析结果的列表，每个字典代表一条记录。如果出错则返回包含错误信息的字典列表。
        """
        # --- 使用 self.system_prompt 并准备最终提示 ---
        # final_system_prompt = self.prompt_manager.prepare_prompt(
        #     system_prompt,
        #     user_prompt_content # 飞书表格数据
        # )
        final_system_prompt = self.system_prompt.replace("{{TRANSACTION}}", user_prompt_content)
        # --- 结束准备提示 ---

        # --- 构建 LangChain 消息列表 ---
        user_message = "请根据系统提示中的信息进行分析并按要求格式输出。" # 与 DeepSeek 类似的用户指令
        messages = [
            SystemMessage(content=final_system_prompt),
            HumanMessage(content=user_message)
        ]
        # --- 结束构建消息列表 ---

        try:
            logging.info(f"Sending request to Gemini model: {self.model_name}")
            # --- 使用消息列表调用 LLM ---
            response = self.llm.invoke(messages)
            # --- 结束调用修改 ---

            # 提取和解析响应内容
            response_text = ""
            # LangChain 的 invoke 通常返回 AIMessage 对象，内容在 .content 属性中
            if hasattr(response, 'content'):
                response_text = response.content
            
            else:
                 logging.warning(f"Warning: Unexpected response type from Gemini invoke: {type(response)}")
                 response_text = str(response) # 尝试转换为字符串

            logging.info("Received response from Gemini.")

            # --- 修改：更稳健地处理可能的非 JSON 输出 (类似 DeepSeek) ---
            response_text = response_text.strip()
            json_match = re.search(r'```json\s*([\s\S]*?)\s*```', response_text, re.DOTALL)
            if json_match:
                json_string = json_match.group(1).strip()
                logging.info("Extracted JSON block from markdown.")
            else:
                # 否则，假设整个响应是 JSON 或包含 JSON
                json_string = response_text
                # 尝试去除可能的非 JSON 前缀/后缀（简单处理）
                first_brace = json_string.find('[') # 预期是列表
                last_brace = json_string.rfind(']')
                if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
                     json_string = json_string[first_brace:last_brace+1]
                     logging.info("Attempting to parse extracted array-like string.")
                else:
                     # 如果找不到数组结构，可能需要更复杂的清理或直接报错
                     logging.warning("Could not reliably find JSON array structure in the response.")

            try:
                analysis_result = json.loads(json_string)
                # 验证返回的是否是列表 (根据 prompt 要求是 JSON 数组)
                if isinstance(analysis_result, list):
                     logging.info(f"Successfully parsed JSON array response from Gemini. Records found: {len(analysis_result)}")
                     return analysis_result
                else:
                     logging.error(f"Gemini response parsed but is not the expected list format. Type: {type(analysis_result)}")
                     # 返回与 DeepSeek 一致的错误格式
                     return [{"error": "LLM response is not a JSON array", "raw_response": response_text}]

            except json.JSONDecodeError as json_err:
                logging.error(f"Failed to parse JSON response from Gemini: {json_err}")
                logging.error(f"Raw content causing error: {response_text}")
                # 返回包含原始响应的错误，格式与 DeepSeek 一致
                return [{"error": f"Failed to parse JSON response: {json_err}", "raw_response": response_text}]
            # --- 结束 JSON 处理修改 ---

        except Exception as e:
            logging.error(f"An error occurred during Gemini API call or processing: {e}")
            import traceback
            traceback.print_exc() # 打印详细的回溯信息
            # 返回与 DeepSeek 一致的错误格式
            return [{"error": f"API call failed: {str(e)}", "raw_response": None}]
    # --- 结束 analyze_dialogue 修改 ---
