import logging
import sys

class Logger(object):
    def __init__(self, name='logger', level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # fh = logging.FileHandler('%s.log' % name, 'w')
        # fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        # fh.setFormatter(fmt)
        # self.logger.addHandler(fh)

        sh = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        sh.setFormatter(fmt)
        self.logger.addHandler(sh)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg):
        self.logger.error(msg)

## Global Logger
logger = Logger()