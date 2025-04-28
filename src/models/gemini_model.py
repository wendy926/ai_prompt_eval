from langchain_google_genai import ChatGoogleGenerativeAI
from pathlib import Path
import json

class PromptManager:
    def __init__(self):
        self.base_path = Path(__file__).parent.parent / "prompts"
        
    def load_prompt(self, filename: str) -> str:
        with open(self.base_path / filename, 'r', encoding='utf-8') as f:
            return f.read()
            
    def prepare_prompt(self, system_prompt: str, user_prompt: str, submitter: str) -> str:
        return system_prompt.replace("{{TRANSACTION}}", user_prompt)\
                          .replace("{{SUBMITTER}}", submitter)

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
    
    def analyze_dialogue(self, submitter: str = "AI prompt") -> dict:
        # Load prompts
        system_prompt = self.prompt_manager.load_prompt("system_prompt.txt")
        user_prompt = self.prompt_manager.load_prompt("user_prompt.txt")
        
        # Prepare final prompt
        final_prompt = self.prompt_manager.prepare_prompt(
            system_prompt,
            user_prompt,
            submitter
        )
        
        # Call the LLM with the final prompt using `invoke`
        response = self.llm.invoke(final_prompt)
        
        # Parse the response text
        response_text = response.strip()
        if response_text.startswith('```json'):
            response_text = response_text[7:-3]
        
        return json.loads(response_text)
