import sys
import signal
import logging
import tornado
from tornado.gen import coroutine
from argparse import ArgumentParser
from katcp import KATCPClientResource
from influxdb import InfluxDBClient

log = logging.getLogger("katcp-exporter")


class KatcpSidecar(object):
    def __init__(self, host, port):
        """
        Constructs a new instance.

        :param      host:  The address of the server to sidecar
        :param      port:  The server port
        """
        log.debug("Constructing sidecar for {}:{}".format(host, port))
        self.rc = KATCPClientResource(dict(
            name="sidecar-client",
            address=(host, port),
            controlled=True))
        self._update_callbacks = set()
        self._previous_sensors = set()

    @coroutine
    def start(self):
        """
        @brief     Start the sidecar
        """
        @coroutine
        def _start():
            log.debug("Waiting on synchronisation with server")
            yield self.rc.until_synced()
            log.debug("Client synced")
            log.debug("Requesting version info")
            response = yield self.rc.req.version_list()
            log.info("response: {}".format(response))
            self.ioloop.add_callback(self.on_interface_changed)
        self.rc.start()
        self.ic = self.rc._inspecting_client
        self.ioloop = self.rc.ioloop
        self.ic.katcp_client.hook_inform(
            "interface-changed",
            lambda message: self.ioloop.add_callback(
                self.on_interface_changed))
        self.ioloop.add_callback(_start)

    def stop(self):
        """
        @brief      Stop the sidecar
        """
        self.rc.stop()

    @coroutine
    def on_interface_changed(self):
        """
        @brief    Synchronise with the sidecar'd servers new sensors
        """
        log.debug("Waiting on synchronisation with server")
        yield self.rc.until_synced()
        log.debug("Client synced")
        current_sensors = set(self.rc.sensor.keys())
        log.debug("Current sensor set: {}".format(current_sensors))
        removed = self._previous_sensors.difference(current_sensors)
        log.debug("Sensors removed since last update: {}".format(removed))
        added = current_sensors.difference(self._previous_sensors)
        log.debug("Sensors added since last update: {}".format(added))
        for name in list(added):
            log.debug(
                "Setting sampling strategy and callbacks on sensor '{}'".format(name))
            self.rc.set_sampling_strategy(name, "auto")
            self.rc.set_sensor_listener(name, self.on_sensor_update)
        self._previous_sensors = current_sensors

    @coroutine
    def on_sensor_update(self, sensor, reading):
        """
        @brief      Callback to be executed on a sensor being updated

        @param      sensor   A KATCP Sensor Object
        @param      reading  The sensor reading
        """
        log.debug("Recieved sensor update for sensor '{}': {}".format(
            sensor.name, repr(reading)))
        for callback in list(self._update_callbacks):
            try:
                callback(sensor, reading)
            except Exception as error:
                log.exception(
                    "Failed to call update callback {} with error: {}".format(
                        callback, str(error)))

    def add_sensor_update_callback(self, callback):
        """
        @brief    Add a sensor update callback.

        @param      callback:  The callback

        @note     The callback must have a call signature of
                  func(sensor, reading)
        """
        self._update_callbacks.add(callback)

    def remove_sensor_update_callback(self, callback):
        """
        @brief    Remove a sensor update callback.

        @param      callback:  The callback
        """
        self._update_callbacks.remove(callback)


class InfluxDBExporter(object):
    """
    An exporter for converting KATCP sensor updates to
    """
    def __init__(self, hostname, port, user,
                 password, db, tags,
                 valid_istates=None):
        """
        @brief    Construct an InfluxDBExporter
        """
        self._client = InfluxDBClient(hostname, port, user, password, db)
        self._client.create_database(db)
        self._tags = tags
        self._valid_istates = [1] if valid_istates is None else valid_istates

    def sensor_udpate_callback(self, sensor, reading):
        if reading.istatus not in self._valid_istates:
            log.debug(
                "Handler ignoring reading with invalid istatus ({})".format(
                    reading.istatus))
            return
        valid_name = sensor.name.replace("-", "_").replace(".", "_")
        json_body = [
            {
                "measurement": valid_name,
                "tags": self._tags,
                "time": int(reading.timestamp * 1e9),
                "fields":
                    {
                        "value": reading.value
                    }
            }]
        log.debug("Exporting '{}'".format(json_body))
        self._client.write_points(json_body)


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
    parser.add_argument('--idb-host', action='store', dest='idb_host',
        type=str, help='The InfluxDB instance address to connect to',
        default="0.0.0.0")
    parser.add_argument('--idb-port', action='store', dest='idb_port',
        type=int,  help='The InfluxDB instance port to connect to',
        default=8086)
    parser.add_argument('--idb-user', action='store', dest='idb_user',
        type=str, help='The InfluxDB username', default="admin")
    parser.add_argument('--idb-pass', action='store', dest='idb_pass',
        type=str, help='The InfluxDB password', default="admin")
    parser.add_argument('--idb-db', action='store', dest='idb_db', type=str,
        help='The InfluxDB database to use')
    parser.add_argument("--idb-tags",
        metavar="KEY=VALUE",
        nargs='+',
        help=("Specify tags for exported data in the form of key-value pairs. "
        "(do not put spaces before or after the = sign). "
        "If a value contains spaces, you should define "
        "it with double quotes: "
        'foo="this is a sentence". Note that '
        "values are always treated as strings."),
        default=[])
    parser.add_argument('--log-level', action='store', dest='log_level', type=str,
        help='Logging level', default="INFO")
    args = parser.parse_args()
    tags = dict(map(lambda s: s.split('='), args.idb_tags))
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
    handler = InfluxDBExporter(
        args.idb_host, args.idb_port, args.idb_user,
        args.idb_pass, args.idb_db, tags)
    client.add_sensor_update_callback(handler.sensor_udpate_callback)

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
