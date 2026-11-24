# encoding=utf8

from typing import Dict, Tuple
from threading import Lock
from sshtunnel import SSHTunnelForwarder
from resx.config import CFG_TUNNEL
from resx.func import get_my_ip
from resx.log import setup_logger

logger = setup_logger(name=__name__)


# 可以通过隧道连接的Client， 隧道基于sshtunnel.SSHTunnelForwarder
class TunnelMixin(object):
    tunnel_cache: Dict[Tuple[str, int], int] = dict()
    tunnel_cache_lock = Lock()

    def __init__(self, ):
        self._tunnel_server = None
        self.host, self.port, self.tunnel = None, None, None

    def mix(self, ):
        self._tunnel_server = None
        if self._use_tunnel():
            with self.tunnel_cache_lock:
                if (self.host, self.port) in self.tunnel_cache:
                    self.host, self.port = 'localhost', self.tunnel_cache[(self.host, self.port)]
                    return
                self._tunnel_server = SSHTunnelForwarder(remote_bind_address=(self.host, self.port), **CFG_TUNNEL)
                self._tunnel_server.start()
                logger.info(f'localhost:{self._tunnel_server.local_bind_port} --> {self.host}:{self.port}')
                self.tunnel_cache[(self.host, self.port)] = self._tunnel_server.local_bind_port
                self.host, self.port = 'localhost', self._tunnel_server.local_bind_port

    def _use_tunnel(self, ) -> bool:
        if self.host is None or self.port is None:
            return False
        if self.host in ['localhost', '127.0.0.1',]:
            return False
        if self.tunnel is not None:
            return self.tunnel
        return not get_my_ip().startswith('10.99')

    # def __del__(self):
    #    if self._tunnel_server:
    #        self._tunnel_server.close()
