# shared_services/deepseek_client.py
import os
import requests
import logging
from typing import Optional

logger = logging.getLogger("deepseek_client")

class DeepSeekClient:
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY")
        if not self.api_key:
            raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
        
        self.base_url = "https://api.deepseek.com/v1"
        logger.info("DeepSeekClient initialized")
    
    def query(self, prompt: str) -> str:
        try:
            logger.info(f"Sending query to DeepSeek API: {prompt[:50]}...")
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7
                },
                timeout=30
            )
            
            response.raise_for_status()
            data = response.json()
            
            answer = data["choices"][0]["message"]["content"]
            logger.info("Successfully received response from DeepSeek API")
            
            return answer
            
        except Exception as e:
            logger.error(f"DeepSeek API error: {e}", exc_info=True)
            return f"Error accessing DeepSeek API: {str(e)}"