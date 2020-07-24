"""
Copyright (c) 2020 Ewan Barr <ebarr@mpifr-bonn.mpg.de>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""
import logging
import coloredlogs
import time
import signal
import random
import json
from tornado.gen import coroutine, sleep, Return
from tornado.ioloop import IOLoop, PeriodicCallback
from katcp import Sensor, AsyncDeviceServer
from katcp.kattypes import request, Str, concurrent_reply

log = logging.getLogger("testserver")


class TestServer(AsyncDeviceServer):
    VERSION_INFO = ("test-server-api", 0, 1)
    BUILD_INFO = ("test-server-implementation", 0, 1, "rc1")
    DEVICE_STATUSES = ["ok", "degraded", "fail"]

    def __init__(self, ip, port):
        """
        @brief       Construct new TestServer instance

        @params  ip       The IP address on which the server should listen
        @params  port     The port that the server should bind to
        """
        super(TestServer, self).__init__(ip, port)

    def start(self):
        """
        @brief  Start the TestServer server
        """
        self._cb = PeriodicCallback(self.auto_update, 1000)
        self._cb.start()
        super(TestServer, self).start()

    def auto_update(self):
        log.debug("Setting new sensor values")
        self._device_status.set_value(random.choice(self.DEVICE_STATUSES))
        self._current_time.set_value(time.time())

    def stop(self):
        super(TestServer, self).stop()
        self._cb.stop()

    def add_sensor(self, sensor):
        log.debug("Adding sensor: {}".format(sensor.name))
        super(TestServer, self).add_sensor(sensor)

    def remove_sensor(self, sensor):
        log.debug("Removing sensor: {}".format(sensor.name))
        super(TestServer, self).remove_sensor(sensor)

    def setup_sensors(self):
        """
        @brief  Set up monitoring sensors.
        """
        self._device_status = Sensor.discrete(
            "device-status",
            description=json.dumps(dict(
                description="Health of device server",
                valid_states=self.DEVICE_STATUSES
                )),
            params=self.DEVICE_STATUSES,
            default="ok",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._device_status)

        self._current_time = Sensor.float(
            "current-time",
            description=json.dumps(dict(
                description="The current time",
                )),
            default=time.time(),
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._current_time)

        self._dummy_sensor = Sensor.string(
            "dummy-sensor",
            description=json.dumps(dict(
                description="A dummy sensor with a mutable value",
                setter="set-dummy-sensor-value",
                timeout=30.0
                )),
            default="",
            initial_status=Sensor.NOMINAL)
        self.add_sensor(self._dummy_sensor)

    @request(Str())
    @concurrent_reply
    @coroutine
    def request_set_dummy_sensor_value(self, req, value):
        """
        @brief   Set the value of the dummy sensor
        """
        try:
            self._dummy_sensor.set_value(value)
            yield sleep(5)
        except Exception as error:
            log.exception(str(error))
            raise Return(("fail", str(error)))
        else:
            raise Return(("ok",))


@coroutine
def on_shutdown(ioloop, server):
    log.info("Shutting down server")
    yield server.stop()
    ioloop.stop()


def main():
    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option(
        '-H', '--host', dest='host', type=str,
        help='Host interface to bind to')
    parser.add_option(
        '-p', '--port', dest='port', type=int,
        help='Port number to bind to')
    parser.add_option(
        '', '--log-level', dest='log_level', type=str,
        help='Port number of status server instance', default="INFO")
    (opts, args) = parser.parse_args()
    logger = logging.getLogger('testserver')
    coloredlogs.install(
        fmt=("[ %(levelname)s - %(asctime)s - %(name)s - "
             "%(filename)s:%(lineno)s] %(message)s"),
        level=opts.log_level.upper(),
        logger=logger)
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = IOLoop.current()
    log.info("Starting TestServer instance")
    server = TestServer(opts.host, opts.port)
    signal.signal(
        signal.SIGINT, lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, server))

    def start_and_display():
        server.start()
        log.info("Listening at {0}, Ctrl-C to terminate server".format(
            server.bind_address))

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
