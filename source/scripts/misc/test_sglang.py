#!/usr/bin/env python3
"""
scripts/misc/test_sglang.py
Simple test script to query the SGLang OpenAI-compatible API server using Python's standard library.
"""

import os
import json
import urllib.request
import urllib.error
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    port = os.getenv("SGLANG_LLM_EXPOSE_PORT", "30000")
    url = f"http://127.0.0.1:{port}/v1/chat/completions"
    
    payload = {
        "model": "default",
        "messages": [
            {"role": "system", "content": "You are a helpful medical AI assistant."},
            {"role": "user", "content": "What are the common symptoms of acute appendicitis?"}
        ],
        "temperature": 0.2,
        "max_tokens": 150
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    print("=================================================================")
    print("🧠 SGLANG INFERENCE API TESTER")
    print(f"🔗 Target Endpoint: {url}")
    print("=================================================================")
    print(f"📤 Sending Prompt: '{payload['messages'][1]['content']}'...")
    
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
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
    print("=================================================================")

if __name__ == "__main__":
    main()
