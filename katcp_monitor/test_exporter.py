import signal
import logging
import tornado
from tornado.gen import coroutine
from argparse import ArgumentParser
from sidecar import KatcpSidecar

log = logging.getLogger("test-exporter")


class TestExporter(object):
    """
    An exporter for converting KATCP sensor updates to
    """
    def __init__(self, valid_istates=None):
        """
        @brief    Construct an InfluxDBExporter
        """
        self._valid_istates = [1] if valid_istates is None else valid_istates

    def sensor_udpate_callback(self, sensor, reading):
        if reading.istatus not in self._valid_istates:
            log.debug(
                "Handler ignoring reading with invalid istatus ({})".format(
                    reading.istatus))
            return
        valid_name = sensor.name.replace("-", "_").replace(".", "_")
        log.debug("Handling update for sensor {} with reading {}".format(
            valid_name, reading))


@coroutine
def on_shutdown(ioloop, client):
    log.info("Shutting down client")
    yield client.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = ArgumentParser(description=usage)
    parser.add_argument('--host', action='store', dest='host', type=str,
        help='The hostname for the KATCP server to connect to')
    parser.add_argument('--port', action='store', dest='port', type=int,
        help='The port number for the KATCP server to connect to')
    parser.add_argument('--log-level', action='store', dest='log_level', type=str,
        help='Logging level', default="INFO")
    args = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('katcp-exporter')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(args.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting KATCPToIGUIConverter instance")
    client = KatcpSidecar(args.host, args.port)
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, client))
    handler = TestExporter()
    client.add_sensor_update_callback(handler.sensor_udpate_callback)

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
