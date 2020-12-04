import signal
import logging
import tornado
import requests
import time
import json
from functools import wraps
from tornado.gen import coroutine, sleep
from tornado.ioloop import PeriodicCallback, IOLoop
from argparse import ArgumentParser
from sidecar import KatcpSidecar

log = logging.getLogger("katcp-monitor")


def retry_on_fail(min_interval=5.0, max_interval=600.0, max_retries=None):
    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            wait = min_interval
            while True:
                try:
                    response = func(*args, **kwargs)
                except requests.ConnectionError as error:
                    log.error(
                        "Unable to connect to IGUI: {}".format(
                            str(error)))
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


def pretty_print_POST(req):
    print('{}\n{}\r\n{}\r\n\r\n{}'.format(
        '-----------START-----------',
        req.method + ' ' + req.url,
        '\r\n'.join('{}: {}'.format(k, v) for k, v in req.headers.items()),
        req.body,
    ))


class IGUIExporter(object):
    """
    An exporter for converting KATCP sensor updates to
    """
    AUTH_URL = "/icom/authorize"
    TREE_URL = "/icom/api/trees"
    LOG_URL = "/icom/api/logs"
    NODE_URL = "/icom/api/nodes"

    TASK_COMPLETE = 0
    TASK_PENDING = 1
    TASK_RUNNING = 2
    TASK_FAILED = 3
    HAS_ERROR = 1
    NO_ERROR = 0

    def __init__(self, url, user,
                 password, device_id,
                 sidecar,
                 polling_interval=1.0,
                 valid_istates=None):
        """
        @brief    Construct an InfluxDBExporter
        """
        self._session = requests.Session()
        self._igui_url = url
        self._task_map = {}
        self._descriptions = {}
        self._device_id = device_id
        self._sidecar = sidecar
        self._polling_interval = polling_interval
        self._valid_istates = [1] if valid_istates is None else valid_istates
        self._ioloop = IOLoop.current()
        self._stop = False
        self.authorize(user, password)
        self.populate_task_map()

    def get_pending_requests(self):
        log.debug("Fectching pending requests")
        response = self._session.get(
            self.url_for(self.NODE_URL, params={
                "parent_name": self._device_id,
                "pending_request": self.TASK_PENDING}))
        response.raise_for_status()
        log.debug("Fectching pending requests took {} seconds".format(
            response.elapsed.total_seconds()))
        log.debug(response.text)
        if not response.text:
            return []
        return response.json()

    def _set_pending_state(self, task, state, error_state, error_message="NONE"):
        log.debug("Setting pending state on {} to {}".format(
            task["node_name"], state))
        response = self._session.put(
            self.url_for(self.NODE_URL, task["node_id"]),
            json={"pending_request": state,
                  "error_state": error_state,
                  "last_error_message": error_message})
        log.debug("Setting pending state took {} seconds".format(
            response.elapsed.total_seconds()))

    @coroutine
    def handle_request(self, task):
        if task["node_name"] not in self._descriptions.keys():
            log.warning(
                "Received a set request for an untracked task: {}".format(
                    task["node_name"]))
            return
        description = self._descriptions[task["node_name"]]
        if not description.get("setter", None):
            message = "Received a set request for a non-settable task: {}".format(
                task["node_name"])
            log.error(message)
            self._set_pending_state(task, self.TASK_FAILED, self.HAS_ERROR, message)
            return
        self._set_pending_state(task, self.TASK_RUNNING, self.NO_ERROR)
        log.info("Setting the value of {} to {}".format(
            task["node_name"], task["desired_value"]))
        try:
            yield self._sidecar.make_request(
                description["setter"],
                task["desired_value"],
                timeout=description["timeout"])
        except Exception as error:
            message = "Unable to set value of {} to {} with error: {}".format(
                task["node_name"], task["desired_value"], str(error))
            log.error(message)
            self._set_pending_state(task, self.TASK_FAILED, self.HAS_ERROR, message)
        else:
            self._set_pending_state(task, self.TASK_COMPLETE, self.NO_ERROR)

    def start(self):
        self._ioloop.add_callback(self.handle_pending_requests)
        pass

    def stop(self):
        self._ioloop.clear_current()
        pass

    @coroutine
    def handle_pending_requests(self):
        try:
            tasks = self.get_pending_requests()
            log.debug("Found {} tasks with pending updates".format(
                len(tasks)))
            for task in tasks:
                if task["pending_request"] == "1":
                    yield self.handle_request(task)
        except Exception as error:
            log.error("Error while handling pending requests: {}".format(
                str(error)))
        finally:
            yield sleep(self._polling_interval)
            self._ioloop.add_callback(self.handle_pending_requests)

    def url_for(self, path, idx=None, params=None):
        params_str = ""
        if params:
            params_str = "?" + "&".join(
                ["{}={}".format(key, value)
                 for key, value in params.items()])
        return '{}{}{}{}'.format(
            self._igui_url,
            path,
            "/{}".format(idx) if idx else "",
            params_str)

    @retry_on_fail(min_interval=5.0, max_interval=600.0, max_retries=None)
    def authorize(self, user, password):
        response = self._session.get(
            self.url_for(self.AUTH_URL),
            auth=(user, password))
        if response.status_code != 200:
            raise Exception(
                "Authorization request failed with status [{}]: {}".format(
                    response.status_code, response.reason))
        elif response.text == 'true':
            log.info("Successfully authorized with IGUI")
        else:
            raise Exception("Unknown exception on login")

    def populate_task_map(self):
        response = self._session.get(
            self.url_for(
                self.TREE_URL,
                self._device_id,
                params={"node_type": "Tasks"}))
        tree = response.json()
        for child in tree["children"]:
            self._task_map[child["data"]["node_name"]] = child["id"]
        log.debug("Recovered task map: \n{}".format(self._task_map))

    def sensor_udpate_callback(self, sensor, reading):
        if reading.istatus not in self._valid_istates:
            log.debug(
                "Handler ignoring reading with invalid istatus ({})".format(
                    reading.istatus))
            return

        try:
            metadata = json.loads(sensor.description)["metadata"]
        except:
            metadata = {"mysql_task_type": "GET"}

        valid_name = sensor.name.replace("-", "_").replace(".", "_")
        if valid_name in self._task_map.keys():
            log.debug("Executing PUT request")
            response = self._session.put(
                self.url_for(self.NODE_URL, self._task_map[valid_name]),
                json={
                    "current_value": reading.value,
                    "metadata": metadata
                })
            log.debug("PUT request took {} seconds for sensor {}".format(
                response.elapsed.total_seconds(), valid_name))
        else:
            log.debug("Executing POST request")
            log.debug("METADATA for sensor {}: {}".format(sensor.name, metadata))
            response = self._session.post(
                self.url_for(self.NODE_URL),
                json={
                    'current_value': reading.value,
                    'metadata': metadata,
                    'node_name': valid_name,
                    'parent_name': self._device_id,
                    'node_type': 'Tasks'})
            log.debug("POST request took {} seconds".format(
                response.elapsed.total_seconds()))
            log.debug(response.json())
            self._task_map[valid_name] = response.json()["node_id"]

        try:
            self._descriptions[valid_name] = json.loads(
                sensor.description)
        except Exception as error:
            log.warning("Could not parse sensor description: {}".format(
                str(error)))


@coroutine
def on_shutdown(ioloop, client, handler):
    handler.stop()
    log.info("Shutting down client")
    yield client.stop()
    ioloop.stop()


def main():
    usage = "usage: %prog [options]"
    parser = ArgumentParser(description=usage)
    parser.add_argument('--host', action='store', dest='host', type=str,
                        help='The hostname for the KATCP server to connect to')
    parser.add_argument(
        '--port', action='store', dest='port', type=int,
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
    parser.add_argument(
        '--log-level',
        action='store',
        dest='log_level',
        type=str,
        help='Logging level',
        default="INFO")
    args = parser.parse_args()
    FORMAT = "[ %(levelname)s - %(asctime)s - %(filename)s:%(lineno)s] %(message)s"
    logger = logging.getLogger('katcp-monitor')
    logging.basicConfig(format=FORMAT)
    logger.setLevel(args.log_level.upper())
    logging.getLogger('katcp').setLevel('INFO')
    ioloop = tornado.ioloop.IOLoop.current()
    log.info("Starting KATCPToIGUIConverter instance")
    client = KatcpSidecar(args.host, args.port)
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: ioloop.add_callback_from_signal(
            on_shutdown, ioloop, client, handler))
    handler = IGUIExporter(
        args.igui_url,
        args.igui_user,
        args.igui_pass,
        args.igui_parent,
        client)
    client.add_sensor_update_callback(handler.sensor_udpate_callback)

    def start_and_display():
        client.start()
        log.info("Ctrl-C to terminate client")
        handler.start()

    ioloop.add_callback(start_and_display)
    ioloop.start()


if __name__ == "__main__":
    main()
