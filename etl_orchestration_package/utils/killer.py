import signal

from loguru import logger


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        if not GracefulKiller.kill_now:
            logger.warning('OK. I will stop in few seconds...')
            self.kill_now = True
            GracefulKiller.kill_now = True
        else:
            logger.warning('OK. Hard stop...')
            exit(0)
