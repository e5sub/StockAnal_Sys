# -*- coding: utf-8 -*-
"""
Input: AI API配置（环境变量）
Output: 统一的OpenAI客户端实例，带超时、重试和错误处理
Pos: app/core/ai_client.py - 所有AI调用的统一入口

一旦我被修改，请更新我的头部注释，以及所属文件夹的md。
"""
import os
import logging
from openai import OpenAI
import httpx

logger = logging.getLogger(__name__)

# 友好错误消息映射
ERROR_MESSAGES = {
    'RateLimitError': '服务繁忙，请稍后重试（API限流）',
    'APITimeoutError': 'AI分析超时，请稍后重试',
    'APIConnectionError': '无法连接AI服务，请检查网络',
    'AuthenticationError': 'AI服务认证失败，请检查API密钥配置',
    'APIStatusError': 'AI服务暂时不可用，请稍后重试',
}


def get_ai_client():
    """获取配置好的OpenAI客户端（带超时和重试）"""
    api_key = os.getenv('OPENAI_API_KEY')
    base_url = os.getenv('OPENAI_API_URL', 'https://api.openai.com/v1')

    if not api_key:
        logger.warning("OPENAI_API_KEY 未配置，AI功能将不可用")
        return None

    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=httpx.Timeout(180.0, connect=10.0),
        max_retries=2,
    )
    return client


def get_ai_model():
    """获取配置的AI模型名称"""
    return os.getenv('OPENAI_API_MODEL', 'gpt-4o')


def chat_completion(client, messages, temperature=0.7, max_tokens=4096, tools=None, tool_choice=None):
    """统一的聊天完成调用，带错误处理"""
    if client is None:
        return None, "AI服务未配置，请设置OPENAI_API_KEY环境变量"

    model = get_ai_model()

    try:
        kwargs = {
            'model': model,
            'messages': messages,
            'temperature': temperature,
            'max_tokens': max_tokens,
        }
        if tools:
            kwargs['tools'] = tools
        if tool_choice:
            kwargs['tool_choice'] = tool_choice

        response = client.chat.completions.create(**kwargs)
        return response, None
    except Exception as e:
        error_type = type(e).__name__
        friendly_msg = ERROR_MESSAGES.get(error_type, f'AI分析出错: {str(e)}')
        logger.error(f"AI调用失败 [{error_type}]: {str(e)}")
        return None, friendly_msg


def get_completion_content(response):
    """从响应中提取文本内容"""
    if response and response.choices:
        return response.choices[0].message.content
    return None
