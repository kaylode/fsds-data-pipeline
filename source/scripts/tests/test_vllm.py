# scripts/misc/test_vllm.py
# A clean, zero-dependency testing script to query your local vLLM OpenAI-compatible API server

import json
import os
import urllib.request
from loguru import logger

def main():
    base = os.getenv("VLLM_URL", f"http://localhost:{os.getenv('VLLM_PORT', '8000')}")
    url = f"{base}/v1/chat/completions"
    headers = {"Content-Type": "application/json"}
    
    # Define query matching vLLM format (serving model at path '/model')
    data = {
        "model": "/model",
        "messages": [
            {"role": "user", "content": "Explain in one sentence what a full-stack data streaming architecture is."}
        ],
        "max_tokens": 80,
        "temperature": 0.7
    }
    
    logger.info(f"📡 Connecting to local vLLM server on {url}...")
    
    try:
        req = urllib.request.Request(
            url, 
            data=json.dumps(data).encode("utf-8"), 
            headers=headers, 
            method="POST"
        )
        
        with urllib.request.urlopen(req, timeout=60) as response:
            res_body = response.read().decode("utf-8")
            res_data = json.loads(res_body)
            reply = res_data["choices"][0]["message"]["content"]
            logger.success(f"🤖 Nemotron Response:\n{reply.strip()}")
            
    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        logger.info("💡 Troubleshooting Checklist:")
        logger.info("  1. Verify the container is running: run 'make ps' inside the compute node.")
        logger.info("  2. Ensure your port forwarder is running in a VS Code terminal: 'python3 scripts/misc/forward_ports.py <compute_node_hostname>'")

if __name__ == "__main__":
    main()
