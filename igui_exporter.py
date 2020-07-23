import signal
import logging
import tornado
import requests
import time
from functools import wraps
from tornado.gen import coroutine
from argparse import ArgumentParser
from katcp import KATCPClientResource

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


def retry_on_fail(min_interval=5.0, max_interval=600.0, max_retries=None):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            wait = min_interval
            while True:
                try:
                    response = func(*args, **kwargs)
                except requests.ConnectionError as error:
                    log.error("Unable to connect to IGUI: {}".format(str(error)))
                    time.sleep(wait)
                    wait += min_interval
                    if wait >= max_interval:
                        wait = max_interval
                        continue
                except Exception as error:
                    log.exception("Unexpected error: {}".format(str(error)))
                    raise error
                else:
                    break
            return response
        return wrapped
    return wrapper


class IGUIExporter(object):
    """
    An exporter for converting KATCP sensor updates to
    """
    AUTH_URL = "/icom/authorize"
    TREE_URL = "/icom/api/trees"
    LOG_URL = "/icom/api/logs"
    NODE_URL = "/icom/api/nodes"

    def __init__(self, url, user,
                 password, device_id,
                 valid_istates=None):
        """
        @brief    Construct an InfluxDBExporter
        """
        self._session = requests.Session()
        self._igui_url = url
        self._task_map = {}
        self._device_id = device_id
        self._valid_istates = [1] if valid_istates is None else valid_istates
        self.authorize(user, password)
        self.populate_task_map()

    def url_for(self, path, idx=None):
        return '{}{}{}'.format(
            self._igui_url,
            path, "/{}".format(idx) if idx else "")

    @retry_on_fail(min_interval=5.0, max_interval=600.0, max_retries=None)
    def authorize(self, user, password):
        response = self._session.get(
                    self.url_for(self.AUTH_URL),
                    auth=(user, password))

        if response.status_code != 200:
            raise Exception("Authorization request failed with status [{}]: {}".format(
                    response.status_code, response.reason))
        elif response.text == 'true':
            log.info("Successfully authorized with IGUI")
        else:
            raise Exception("Unknown exception on login")

    def populate_task_map(self):
        response = self._session.get(self.url_for(self.TREE_URL, self._device_id))
        tree = response.json()
        for child in tree["children"]:
            self._task_map[child["data"]["node_name"]] = child["id"]

    def sensor_udpate_callback(self, sensor, reading):
        if reading.istatus not in self._valid_istates:
            log.debug(
                "Handler ignoring reading with invalid istatus ({})".format(
                    reading.istatus))
            return
        valid_name = sensor.name.replace("-", "_").replace(".", "_")

        if valid_name in self._task_map.keys():
            response = self._session.put(
                self.url_for(self.NODE_URL, self._task_map[valid_name]),
                json={"current_value": reading.value})
        else:
            response = self._session.post(
                self.url_for(self.NODE_URL),
                json={
                    'current_value': reading.value,
                    'metadata': {
                        'mysql_task_type': 'GET'
                        },
                    'node_name': valid_name,
                    'parent_name': self._device_id})
            log.debug(response.json())
            self._task_map[valid_name] = response.json()["node_id"]


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
    parser.add_argument('--igui-url', action='store', dest='igui_url',
        type=str, help='The IGUI URL address to connect to',
        default="http://localhost:30001")
    parser.add_argument('--igui-user', action='store', dest='igui_user',
        type=str, help='The IGUI username', default="admin")
    parser.add_argument('--igui-pass', action='store', dest='igui_pass',
        type=str, help='The IGUI password', default="admin")
    parser.add_argument('--igui-parent', action='store', dest='igui_parent',
        type=str, help='The IGUI parent ID')
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
    client = client = KatcpSidecar(args.host, args.port)
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, client))
    handler = IGUIExporter(args.igui_url, args.igui_user, args.igui_pass, args.igui_parent)
    client.add_sensor_update_callback(handler.sensor_udpate_callback)

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
