# encoding=utf8
import os
import time
import base64
import requests
from typing import Dict, Optional
from threading import Lock, current_thread
from requests import Session, Response
import urllib3
from resx.log import setup_logger

logger = setup_logger(name=__name__)

urllib3.disable_warnings()


def _default_proxy() -> Optional[Dict[str, str]]:
    http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
    https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
    if http_proxy or https_proxy:
        return {'http': http_proxy, 'https': https_proxy}
    return None


PROXY_DEFAULT = _default_proxy()
HEADERS_DEFAULT = {
    'User-Agent': 'Mozilla/5.0 (compatible; ResX-Client)',
    'Accept-Language': 'zh-CN,zh',
}


class URLPat(object):
    def __init__(
            self,
            name: str,
            pat: str,
            validate_func=None,
            timeout=3.,
            tries=3,
            **kwargs,
    ):
        self.name = name
        self.pat = pat
        self.validate_func = validate_func or self.response_validate_default
        self.timeout = timeout
        self.tries = tries
        self.custom_headers = kwargs

    @staticmethod
    def response_validate_default(response: Response, ctx=None):  # noqa
        if response.status_code >= 400:
            return None
        if len(response.text) > 0:
            return True
        return False


class ReqManager(object):
    def __init__(
            self,
            pats: list[URLPat],
            proxies=None,
            default_headers=None,
            use_proxy=True,
    ):
        self.sessions: Dict[int, Session] = {}
        self.sessions_lock = Lock()
        self.proxies = (proxies or PROXY_DEFAULT) if use_proxy else None
        self.default_headers = default_headers or HEADERS_DEFAULT
        self.pats = dict((x.name, x) for x in pats)

    def request(self, pat: str, force_post=False, response_validate_ctx=None, **kwargs) -> Optional[Response]:
        with self.sessions_lock:
            tid = current_thread().ident
            if tid not in self.sessions:
                self.sessions[tid] = Session()
            if self.sessions[tid] is None:
                self.sessions[tid] = Session()
            session = self.sessions[tid]

        pat_obj = self.pats[pat]
        url = pat_obj.pat
        for k, v in kwargs.copy().items():
            if '{' + k + '}' in url:
                url = url.replace('{' + k + '}', str(v))
                kwargs.pop(k)

        for try_id in range(pat_obj.tries):
            start_ts = time.time()
            try:
                response = session.request(
                    method='POST' if (kwargs or force_post) else 'GET',
                    url=url,
                    data=kwargs or {},
                    timeout=pat_obj.timeout,
                    headers=self.default_headers | (pat_obj.custom_headers or {}),
                    proxies=self.proxies,
                    verify=False,
                )
                proxy = response.cookies.get_dict().get('proxyBase', '')
                if proxy:
                    proxy = base64.standard_b64decode(proxy).decode()
                cost_ts = time.time() - start_ts
                status_code, size = response.status_code, len(response.content)
                validate_ret = pat_obj.validate_func(response, response_validate_ctx)
                logger.info(f'RESPONSE #{try_id} {cost_ts:.1f} {status_code} {size:5d} {url} {proxy:20s}')
                if validate_ret:
                    return response
                if proxy:
                    del session.cookies['proxyBase']
            except requests.exceptions.ReadTimeout:
                logger.info(f'RESPONSE #{try_id} {url} ReadTimeout')
        logger.warning(f'{url} max tries={pat_obj.tries} exceed!')
        return None