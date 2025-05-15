import requests

def test_siliconflow_api(api_key):
    """最简单的SiliconFlow API测试脚本"""
    url = "https://api.siliconflow.cn/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    data = {
        "model": "Qwen/QwQ-32B",
        "messages": [{
            "role": "user",
            "content": "请用一句话回答：中国的首都是哪里？"
        }],
        "temperature": 0.3
    }

    try:
        print("正在发送测试请求...")
        response = requests.post(url, headers=headers, json=data, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        answer = result["choices"][0]["message"]["content"]
        print(f"✅ 测试成功！AI回答: {answer}")
        return True
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("❌ 认证失败：请检查API密钥是否正确")
        else:
            print(f"❌ 请求失败：HTTP {e.response.status_code}")
        return False
    except Exception as e:
        print(f"❌ 发生错误：{str(e)}")
        return False

# 直接在这里填写您的API密钥（替换sk-xxx为真实密钥）
YOUR_API_KEY = "sk-ornolnlzsikbftfwaaggnlmerndnhaiznatstubnpfpyaamg"  

# 运行测试
if __name__ == "__main__":
    print("=== SiliconFlow API简易测试 ===")
    test_siliconflow_api(YOUR_API_KEY)