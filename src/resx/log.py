# encoding=utf8
import os
import sys
import inspect
import logging.config

try:
    from crawler_log import log_init_v2  # type: ignore
    _HAS_CRAWLER_LOG = True
except ImportError:
    _HAS_CRAWLER_LOG = False

from resx.func import get_env, ResXEnv


# https://github.com/Delgan/loguru?tab=readme-ov-file#entirely-compatible-with-standard-logging
# https://loguru.readthedocs.io/en/stable/_modules/loguru/_logger.html#Logger.add
class _InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Get corresponding Loguru level if it exists.
        # level: str | int
        from loguru import logger
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


# crawler_log refer to https://b3sh6jivuw.feishu.cn/docx/FgHndGt8gofRLLxjfQrcaz0SnWb
# 统一的日志门面
def setup_logger(
        logger_path=None,  # 日志文件路径，默认使用标准输出作为日志
        backup_count=3,  # 备份数 当logger_path有值时有效
        rotate_mode=None,  # 按X备份H/D Hour or Day
        process_safe=False,  # 进程安全
        debug=False,  # debug or info
        use_crawler_log=False,  # use crawler_log or not
        crawler_log_enable=False,  # 是否打开统一日志  废弃， 通过环境配置读取
        name='',  # logger name
        domain=None,
        dimension=None,
):
    crawler_log_enable = (get_env() in [ResXEnv.ONLINE, ResXEnv.YUFA])
    logger_obj = os.environ.get('logger')
    if logger_obj == 'crawler_log' and _HAS_CRAWLER_LOG:
        return log_init_v2.getLogger(name)
    if logger_obj == 'loguru':
        return logging.getLogger(name)
    if use_crawler_log and _HAS_CRAWLER_LOG:
        os.environ.setdefault('logger', 'crawler_log')
        return log_init_v2.Log(
            module_name=name,
            env='k8s' if crawler_log_enable else 'other',
            domain=domain or name,
            dimension=dimension or name,
            level=logging.INFO if not debug else logging.DEBUG,
            formatter=logging.Formatter('%(levelname)-6s %(threadName)10s %(asctime)s %(filename)s:%(lineno)d - %(message)s', style='%'))  # noqa

    logging.basicConfig(handlers=[_InterceptHandler()], level=0, force=True)
    fmt_str = '<c>{level:6s} {thread.name:10s} {time:YYYY-MM-DD HH:mm:ss} {file}:{line}</c> - {message}'

    logger_kwargs = dict(format=fmt_str, enqueue=process_safe, level=('INFO' if not debug else 'DEBUG'))
    stdout_logger_kwargs = dict(format=fmt_str, level='INFO', sink=sys.stdout)
    if not logger_path:
        logger_kwargs['sink'] = sys.stdout
        handlers = [logger_kwargs, ]
    else:
        handlers = [logger_kwargs, stdout_logger_kwargs]
        sink = logger_path  # f'{get_pygs_dir()}/logs/{app_name}.log'
        rotation = None
        if rotate_mode == 'D':
            sink += '.{time:YYYY_MM_DD}'
            rotation = '00:00'
        elif rotate_mode == 'H':
            sink += '.{time:YYYY_MM_DD_HH}'
            rotation = '1h'

        logger_kwargs.update(sink=sink)
        logger_kwargs.update(encoding='utf8')
        logger_kwargs.update(rotation=rotation)
        logger_kwargs.update(retention=f'{backup_count} days')

    from loguru import logger
    # logger.add(**logger_kwargs)
    logger.configure(handlers=handlers)
    os.environ.setdefault('logger', 'loguru')
    return logger