#!/usr/bin/env python3
"""
scripts/misc/test_sglang.py
Premium test script to query the SGLang OpenAI-compatible API server.
Supports dynamic command-line parameters for port, prompt, and system instruction.
"""

import os
import json
import argparse
import urllib.request
import urllib.error
from dotenv import load_dotenv

def main():
    # Load environment variables from .env
    load_dotenv()
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Query SGLang OpenAI-compatible API server.")
    parser.add_argument(
        "--prompt", 
        type=str, 
        default="What are the common symptoms of acute appendicitis?",
        help="The query prompt to send to the model."
    )
    parser.add_argument(
        "--system", 
        type=str, 
        default="You are a helpful medical AI assistant.",
        help="System instructions for the model."
    )
    parser.add_argument(
        "--port", 
        type=str, 
        default=os.getenv("SGLANG_LLM_EXPOSE_PORT", "30000"),
        help="The exposed port of the SGLang server."
    )
    parser.add_argument(
        "--temperature", 
        type=float, 
        default=0.2,
        help="Sampling temperature."
    )
    parser.add_argument(
        "--max-tokens", 
        type=int, 
        default=250,
        help="Maximum tokens to generate."
    )
    parser.add_argument(
        "--host", 
        type=str, 
        default=os.getenv("SGLANG_LLM_EXPOSE_HOST", "g129"),
        help="The exposed host of the SGLang server."
    )
    
    args = parser.parse_args()
    
    url = f"http://{args.host}:{args.port}/v1/chat/completions"
    
    payload = {
        "model": "default",
        "messages": [
            {"role": "system", "content": args.system},
            {"role": "user", "content": args.prompt}
        ],
        "temperature": args.temperature,
        "max_tokens": args.max_tokens
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    print("=================================================================")
    print("🧠 SGLANG INFERENCE API TESTER")
    print(f"🔗 Target Endpoint: {url}")
    print("=================================================================")
    print(f"👤 System Instruct: '{args.system}'")
    print(f"📤 Sending Prompt:  '{args.prompt}'...")
    print("=================================================================")
    
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req, timeout=45) as response:
            res_body = response.read().decode("utf-8")
            data = json.loads(res_body)
            
            print("\n📥 Response Received:")
            print("-----------------------------------------------------------------")
            if "choices" in data and len(data["choices"]) > 0:
                print(data["choices"][0]["message"]["content"])
            else:
                print(json.dumps(data, indent=2))
            print("-----------------------------------------------------------------")
            print("✅ Test execution successful!")
            
    except urllib.error.URLError as e:
        print(f"\n❌ Request failed: {e}")
        print("💡 Tip: Please ensure that the SGLang container is running and healthy (make sglang-logs).")
        print("   If it was just started, it might still be pulling images or installing packages.")
    print("=================================================================")

if __name__ == "__main__":
    main()
