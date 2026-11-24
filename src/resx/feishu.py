# encoding=utf8

from typing import List
import json
import os
import requests
from requests_toolbelt import MultipartEncoder
from resx.log import setup_logger

logger = setup_logger(name=__name__)


# ---------------------------------------------------------------------------
# at 人员 open_id 映射表，按需覆盖
# open_id_dict = {
#     '张三': 'ou_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
# }
# ---------------------------------------------------------------------------
open_id_dict: dict = {}


def _app_id():
    return os.environ.get('FEISHU_APP_ID', '')


def _app_secret():
    return os.environ.get('FEISHU_APP_SECRET', '')


def send_feishu_message(
        ats: List[str] = None,
        text: str = '',
        url: str = '',
) -> bool:
    content_text = ''.join(
        f'<at user_id="{open_id_dict[at]}"></at>' for at in (ats or []) if at in open_id_dict
    )
    content_text += '\n' + text
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload = {
        "msg_type": "text",
        "content": {"text": content_text},
    }
    response = requests.post(url=url, data=json.dumps(payload), headers=headers)
    logger.info(f'{response.status_code} {response.text}')
    return True


def get_tenant_access_token(app_id=None, app_secret=None):
    app_id = app_id or _app_id()
    app_secret = app_secret or _app_secret()
    url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
    headers = {"Content-Type": "application/json; charset=utf-8"}
    payload_data = {"app_id": app_id, "app_secret": app_secret}
    response = requests.post(url=url, data=json.dumps(payload_data), headers=headers).json()
    return response['tenant_access_token']


def upload_file(file, app_id=None, app_secret=None, file_name='1.pdf'):
    app_id = app_id or _app_id()
    app_secret = app_secret or _app_secret()
    url = "https://open.feishu.cn/open-apis/im/v1/files"
    multi_form = MultipartEncoder({'file_type': 'stream', 'file_name': file_name, 'file': file})
    headers = {
        'Authorization': f'Bearer {get_tenant_access_token(app_id, app_secret)}',
        'Content-Type': multi_form.content_type,
    }
    response = requests.post(url, headers=headers, data=multi_form)
    try:
        key = response.json()['data']['file_key']
    except Exception:
        logger.warning(f'error get key {response}')
        return None
    logger.info(f'{key} {multi_form.content_type}')
    return key


def upload_image(image, app_id=None, app_secret=None):
    app_id = app_id or _app_id()
    app_secret = app_secret or _app_secret()
    url = "https://open.feishu.cn/open-apis/im/v1/images"
    multi_form = MultipartEncoder({'image_type': 'message', 'image': image})
    headers = {
        'Authorization': f'Bearer {get_tenant_access_token(app_id, app_secret)}',
        'Content-Type': multi_form.content_type,
    }
    response = requests.post(url, headers=headers, data=multi_form)
    try:
        key = response.json()['data']['image_key']
    except Exception:
        logger.warning(f'error get key {response}')
        return None
    logger.info(f'{key} {multi_form.content_type}')
    return key


def send_feishu_message_attachments_sub(chat_id, msg_type, msg_content, app_id=None, app_secret=None):
    app_id = app_id or _app_id()
    app_secret = app_secret or _app_secret()
    url = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
    payload = json.dumps({
        "receive_id": chat_id,
        "msg_type": msg_type,
        "content": json.dumps(msg_content),
    })
    headers = {
        'Authorization': f'Bearer {get_tenant_access_token(app_id, app_secret)}',
        'Content-Type': 'application/json',
    }
    response = requests.post(url, headers=headers, data=payload)
    logger.info(f'{response.status_code} {response.text}')
    return True


def send_feishu_message_attachments(
        chat_id: str, ats=None, text='', image=None, file=None, file_name='1.txt',
        app_id=None, app_secret=None,
) -> bool:
    if ats or len(text) > 0:
        content_text = ''.join(
            f'<at user_id="{open_id_dict[at]}"></at>' for at in (ats or []) if at in open_id_dict
        )
        content_text += '\n' + text
        send_feishu_message_attachments_sub(chat_id, 'text', {'text': content_text}, app_id, app_secret)
    if image:
        image_key = upload_image(image, app_id, app_secret)
        if not image_key:
            logger.warning(f'no image_key for image')
            return False
        send_feishu_message_attachments_sub(chat_id, 'image', {'image_key': image_key}, app_id, app_secret)
    if file:
        file_key = upload_file(file, app_id, app_secret, file_name=file_name)
        if not file_key:
            logger.warning(f'no file_key for file')
            return False
        send_feishu_message_attachments_sub(chat_id, 'file', {'file_key': file_key}, app_id, app_secret)
    return True