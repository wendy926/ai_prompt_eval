from langchain_google_genai import ChatGoogleGenerativeAI
from pathlib import Path
import json

class PromptManager:
    def __init__(self):
        self.base_path = Path(__file__).parent.parent / "prompts"
        
    def load_prompt(self, filename: str) -> str:
        with open(self.base_path / filename, 'r', encoding='utf-8') as f:
            return f.read()
            
    def prepare_prompt(self, system_prompt: str, user_prompt: str) -> str:
        return system_prompt.replace("{{TRANSACTION}}", user_prompt)

class GeminiDialogueAnalyzer:
    def __init__(self, api_key: str):
        # Initialize LangChain's ChatGoogleGenerativeAI
        self.llm = ChatGoogleGenerativeAI(
            google_api_key=api_key,
            model="gemini-2.5-pro-preview-03-25",  # Updated model name
            temperature=0.7,
            max_tokens=1024
        )
        self.prompt_manager = PromptManager()
    
    # --- 修改：添加 user_prompt_content 参数 ---
    def analyze_dialogue(self, user_prompt_content: str) -> dict:
        # 加载系统提示模板
        system_prompt = self.prompt_manager.load_prompt("system_prompt.txt")

        # 准备最终的提示内容
        # prepare_prompt 会将 user_prompt_content 替换 {{TRANSACTION}}
        final_prompt = self.prompt_manager.prepare_prompt(
            system_prompt,
            user_prompt_content # 飞书表格数据
        )

        # 使用最终构建好的提示调用 LLM
        try:
            # LangChain 的 ChatGoogleGenerativeAI 可以直接处理字符串输入
            response = self.llm.invoke(final_prompt)

            # 提取和解析响应内容
            response_text = ""
            # LangChain 的 invoke 通常返回 AIMessage 对象，内容在 .content 属性中
            if hasattr(response, 'content'):
                response_text = response.content
            elif isinstance(response, str): # 兼容旧版本或不同配置
                 response_text = response
            else:
                 logging.warning(f"Warning: Unexpected response type from Gemini invoke: {type(response)}")
                 response_text = str(response) # 尝试转换为字符串

            response_text = response_text.strip()
            # 移除可能的 Markdown 代码块标记
            if response_text.startswith('```json'):
                response_text = response_text[7:-3].strip()
            elif response_text.startswith('```'):
                 response_text = response_text[3:-3].strip()

            return json.loads(response_text)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON response from Gemini: {e}")
            logging.error(f"Raw response text: {response_text}")
            return {"error": "Failed to parse LLM response as JSON", "raw_response": response_text}
        except Exception as e:
            logging.error(f"Error during Gemini LLM invocation or processing: {e}")
            import traceback
            traceback.print_exc()
            return {"error": f"An unexpected error occurred with Gemini: {str(e)}"}
