import logging
import json
import re # <--- 添加这一行
from openai import OpenAI # 使用 openai 库与 DeepSeek 兼容的 API 交互
from typing import Optional, Dict, Any, List

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class DeepSeekDialogueAnalyzer:
    """使用 DeepSeek 模型分析对话内容的类"""

    def __init__(
        self,
        api_key: str,
        model_name: str,
        system_prompt: str, # <--- 添加 system_prompt 参数
        base_url: Optional[str] = "https://api.deepseek.com/v1", # DeepSeek API 的基础 URL
        temperature: float = 0.7,
        max_output_tokens: int = 2048
    ):
        """
        初始化 DeepSeekDialogueAnalyzer。

        Args:
            api_key (str): DeepSeek API 密钥。
            model_name (str): 要使用的 DeepSeek 模型名称 (例如 'deepseek-chat')。
            system_prompt (str): 用于指导模型的系统提示。 # <--- 添加文档说明
            base_url (Optional[str]): DeepSeek API 的基础 URL。
            temperature (float): 控制生成文本的随机性。
            max_output_tokens (int): 生成响应的最大 token 数。
        """
        if not api_key:
            raise ValueError("DeepSeek API key is required.")
        if not model_name:
            raise ValueError("DeepSeek model name is required.")
        if not system_prompt: # <--- 添加检查
             raise ValueError("System prompt is required.")

        self.api_key = api_key
        self.model_name = model_name
        self.base_url = base_url
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.system_prompt = system_prompt # <--- 存储 system_prompt
        try:
            # 初始化 OpenAI 客户端，指向 DeepSeek API
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
            logging.info(f"DeepSeek client initialized successfully for model: {self.model_name}")
        except Exception as e:
            logging.error(f"Failed to initialize DeepSeek client: {e}")
            raise ConnectionError(f"Failed to initialize DeepSeek client: {e}")

    # --- 修改：更新方法签名以接收 submitter ---
    def analyze_dialogue(self, user_prompt_content: str) -> List[Dict[str, Any]]:
        """
        使用 DeepSeek 模型分析提供的对话内容。

        Args:
            user_prompt_content (str): 包含对话内容的 JSON 字符串。
            submitter (str): 提交者信息。 # <--- 新增参数说明

        Returns:
            List[Dict[str, Any]]: 分析结果的列表，每个字典代表一条记录。如果出错则返回包含错误信息的字典。
        """
        # --- 修改：只替换 TRANSACTION ---
        final_system_prompt = self.system_prompt.replace("{{TRANSACTION}}", user_prompt_content)
        # --- 结束修改 ---

        # --- 修改：添加 user role 消息 ---
        # user_message 可以是一个简单的指令，因为主要信息已在 system prompt 中
        user_message = "请根据系统提示中的信息进行分析并按要求格式输出。"

        messages = [
            {"role": "system", "content": final_system_prompt},
            {"role": "user", "content": user_message} # <--- 添加 user message
        ]
        # --- 结束修改 ---

        try:
            logging.info(f"Sending request to DeepSeek model: {self.model_name}")
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_output_tokens
                # 移除 response_format 参数
                # response_format={"type": "json_object"}
            )

            # 检查响应是否有效以及是否包含 choices
            if response and response.choices:
                # 获取模型响应内容
                model_response_content = response.choices[0].message.content
                logging.info("Received response from DeepSeek.")
                # logging.debug(f"Raw DeepSeek response content: {model_response_content}") # Debugging

                # --- 修改：更稳健地处理可能的非 JSON 输出 ---
                # 尝试找到 JSON 代码块（如果模型用 markdown 包裹了）
                json_match = re.search(r'```json\s*([\s\S]*?)\s*```', model_response_content, re.DOTALL)
                if json_match:
                    json_string = json_match.group(1).strip()
                    logging.info("Extracted JSON block from markdown.")
                else:
                    # 否则，假设整个响应是 JSON 或包含 JSON
                    json_string = model_response_content.strip()
                    # 尝试去除可能的非 JSON 前缀/后缀（简单处理）
                    first_brace = json_string.find('[')
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
                         logging.info(f"Successfully parsed JSON array response from DeepSeek. Records found: {len(analysis_result)}")
                         return analysis_result
                    else:
                         logging.error(f"DeepSeek response parsed but is not the expected list format. Type: {type(analysis_result)}")
                         return [{"error": "LLM response is not a JSON array", "raw_response": model_response_content}]

                except json.JSONDecodeError as json_err:
                    logging.error(f"Failed to parse JSON response from DeepSeek: {json_err}")
                    logging.error(f"Raw content causing error: {model_response_content}")
                    # 返回包含原始响应的错误
                    return [{"error": f"Failed to parse JSON response: {json_err}", "raw_response": model_response_content}]
                # --- 结束修改 ---
            else:
                logging.error("Invalid or empty response received from DeepSeek.")
                # 尝试获取原始响应信息
                raw_resp_info = str(response) if response else "No response object"
                return [{"error": "Invalid or empty response from LLM", "raw_response": raw_resp_info}]

        except Exception as e:
            logging.error(f"An error occurred during DeepSeek API call: {e}")
            import traceback
            traceback.print_exc() # 打印详细的回溯信息
            return [{"error": f"API call failed: {str(e)}", "raw_response": None}]