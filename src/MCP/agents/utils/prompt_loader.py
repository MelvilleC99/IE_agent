import os
import logging

logger = logging.getLogger("prompt_loader")

def load_prompt(filename: str) -> str:
    """
    Load a prompt from a file in the prompts directory.
    
    Args:
        filename: Name of the prompt file
        
    Returns:
        The prompt text
    """
    try:
        prompt_dir = os.path.join(os.path.dirname(__file__), "..", "prompts")
        prompt_path = os.path.join(prompt_dir, filename)
        
        if not os.path.exists(prompt_path):
            logger.warning(f"Prompt file not found: {prompt_path}")
            return ""
            
        with open(prompt_path, 'r') as f:
            return f.read().strip()
    except Exception as e:
        logger.error(f"Error loading prompt {filename}: {e}")
        return "" 