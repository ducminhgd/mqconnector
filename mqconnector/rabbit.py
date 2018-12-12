# coding=utf-8
"""Connector for Rabbit MQ, using pika module
"""
import json
import time
import logging
import requests

import pika

LOGGER = logging.getLogger('main')

AMQP_URI_FIELD_NAME = 'AMQP_URI'
REST_URI_FIELD_NAME = 'REST_URI'


class AMQPConnector(object):
    """Connect via AMQP"""

    __connection = None
    __max_attempt_retry = 3

    def __init__(self, amqp_uri: str = None, host: str = None, port: int = 5672, vhost: str = None,
                 user: str = None, password: str = None, **options):
        if amqp_uri is not None:
            self.__parameters = pika.URLParameters(amqp_uri)
        else:
            credentials = pika.PlainCredentials(username=user, password=password)
            self.__parameters = pika.ConnectionParameters(host=host, port=port, virtual_host=vhost,
                                                          credentials=credentials, **options)
        if options.get('max_attempt_retry') is not None:
            self.__max_attempt_retry = max(self.__max_attempt_retry, 0)

    def connect(self, attempts_retry: int = None) -> pika.BlockingConnection:
        """
        Connect to message broker and return connection
        :param attempts_retry:
        :return:
        """
        if attempts_retry is None:
            attempts_retry = self.__max_attempt_retry
        else:
            attempts_retry = min(attempts_retry, self.__max_attempt_retry)
        attempts = 0
        while True:
            attempts += 1
            try:
                self.__connection = pika.BlockingConnection(parameters=self.__parameters)
                LOGGER.info('Connected: {attempts} | {host}:{port}/{vhost}'.format(
                    attempts=attempts,
                    host=self.__parameters.host,
                    port=self.__parameters.port,
                    vhost=self.__parameters.virtual_host
                ))
                break
            except:
                LOGGER.warning('Retry connect: {attempts} | {host}:{port}/{vhost}'.format(
                    attempts=attempts,
                    host=self.__parameters.host,
                    port=self.__parameters.port,
                    vhost=self.__parameters.virtual_host
                ))
                if attempts > attempts_retry:
                    LOGGER.exception(
                        'Cannot connect to queue {host}:{port}/{vhost} after {attempts}'.format(
                            attempts=attempts,
                            host=self.__parameters.host,
                            port=self.__parameters.port,
                            vhost=self.__parameters.virtual_host
                        ))
                    break
                time.sleep(min(attempts * 2, 30))
        return self.__connection


class RESTConnector(object):
    """Connector via RESTFul Connector"""
    __host = None
    __virtual_host = None
    __username = None
    __password = None
    __protocol = None

    @property
    def host(self) -> str:
        return self.__host

    @host.setter
    def host(self, value: str):
        self.__host = value

    @property
    def virtual_host(self) -> str:
        return self.__virtual_host

    @property
    def port(self):
        return self.__port

    @port.setter
    def port(self, value):
        self.__port = int(value) if value is not None else None

    @virtual_host.setter
    def virtual_host(self, value: str):
        self.__virtual_host = value

    @property
    def username(self) -> str:
        return self.__username

    @username.setter
    def username(self, value: str):
        self.__username = value

    @property
    def password(self) -> str:
        return self.__password

    @password.setter
    def password(self, value: str):
        self.__password = value

    @property
    def protocol(self) -> str:
        return self.__protocol

    @protocol.setter
    def protocol(self, value: str):
        self.__protocol = value

    def __init__(self, host: str = None, port: int = None, vhost: str = None, username: str = None,
                 password: str = None, use_ssl: bool = True):
        """
        Constructor for RESTFul API connector
        :param host: hostname of API
        :param username: username for authentication if any
        :param password: password for authentication if any
        :param use_ssl: If true, using HTTPS. Unless use HTTP
        """
        self.use_ssl(use_ssl)

        self.host = host
        self.port = port
        self.virtual_host = vhost
        self.username = username
        self.password = password

    def use_ssl(self, use_ssl: bool = True):
        """
        Use SSL or not
        :param use_ssl: If True then use HTTPS, if not then use HTTP
        :return:
        """
        if use_ssl is None:
            return
        if use_ssl:
            self.protocol = 'https'
        else:
            self.protocol = 'http'

    def get_parameters_from_amqp_uri(self, amqp_uri: str):
        """
        Get parameters from AMQP URI
        :param amqp_uri: URI
        :return:
        """
        parameters = pika.URLParameters(amqp_uri)
        self.host = parameters.host
        self.virtual_host = parameters.virtual_host
        self.username = parameters.credentials.username
        self.password = parameters.credentials.password

    def get_api_url(self, api: str = None) -> str:
        """
        Get RESTFul URI host
        :return:
        """
        host_and_port = self.host
        if self.port is not None:
            host_and_port = ':' + str(self.port)
        if api is None:
            return '{protocol}://{host}/api'.format(protocol=self.protocol, host=host_and_port)
        return '{protocol}://{host}/api/{api}'.format(protocol=self.protocol, host=host_and_port,
                                                      api=api)

    def __str__(self) -> str:
        return json.dumps({
            'host': self.host,
            'port': self.port,
            'virtual_host': self.virtual_host,
            'username': self.username,
            'password': '********',
            'protocol': self.protocol,
        })

    def make_get_request(self, url: str) -> str:
        """
        Call request GET
        :param url: URL to call GET request
        :return:
        """
        response = requests.get(url, auth=(self.username, self.password))
        if response.status_code != 200:
            LOGGER.error('Request to {url} | status_code: {status} | body: {body}'.format(url=url,
                                                                                          status=response.status_code,
                                                                                          body=response.text))
            return None
        return response.text

    def get_list_vhosts(self, name_only: bool = True) -> list:
        """
        Get list of Virtual Hosts
        :param name_only: True: get name only
        :return:
        """
        url = self.get_api_url('vhosts')
        data = self.make_get_request(url)
        if data is None:
            return []
        data = json.loads(data)
        if not name_only:
            return data
        result = []
        for row in data:
            result.append(row['name'])
        return result

    def get_overview(self) -> dict:
        """
        Get Overview information
        :return:
        """
        url = self.get_api_url('overview')
        data = self.make_get_request(url)
        if data is None:
            return {}
        data = json.loads(data)
        result = {
            'object_totals': data['object_totals'],
            'message_stats': data['message_stats'],
            'queue_totals': data['queue_totals'],

        }
        return result

    def get_connections(self, vhost: str = None, summary: bool = True) -> list:
        """
        Get connections information
        :return:
        """
        if vhost is None:
            url = self.get_api_url('connections')
        else:
            url = self.get_api_url('vhosts/{vhost}/connections'.format(vhost=vhost))
        data = self.make_get_request(url)
        if data is None:
            return []
        data = json.loads(data)
        if not summary:
            return data
        result = []
        for row in data:
            result.append({
                'vhost': row.get('vhost', None),
                'user': row.get('user', None),
                'name': row.get('name', None),
                'peer_host': row.get('peer_host', None),
                'peer_port': row.get('peer_port', None),
                'host': row.get('host', None),
                'port': row.get('port', None),
                'client_properties': row.get('client_properties', None),
                'channel_max': row.get('channel_max', None),
                'timeout': row.get('timeout', None),
                'protocol': row.get('protocol', None),
                'auth_mechanism': row.get('auth_mechanism', None),
                'channels': row.get('channels', None),
                'state': row.get('state', None),
                'send_pend': row.get('send_pend', None),
                'send_cnt': row.get('send_cnt', None),
                'recv_cnt': row.get('recv_cnt', None),
                'connected_at': row.get('connected_at', None),
            })
        return result

    def get_consumers(self, vhost: str = None) -> list:
        """
        Get consumers information
        :return:
        """
        if vhost is None:
            url = self.get_api_url('consumers')
        else:
            url = self.get_api_url('consumers/{vhost}'.format(vhost=vhost))
        data = self.make_get_request(url)
        if data is None:
            return []
        data = json.loads(data)
        return data

    def get_exchanges(self, vhost: str = None) -> list:
        """
        Get exchanges information
        :return:
        """
        if vhost is None:
            url = self.get_api_url('exchanges')
        else:
            url = self.get_api_url('exchanges/{vhost}'.format(vhost=vhost))
        data = self.make_get_request(url)
        if data is None:
            return []
        data = json.loads(data)
        result = []
        for row in data:
            result.append({
                'name': row.get('name', None),
                'vhost': row.get('vhost', None),
                'type': row.get('type', None),
                'durable': row.get('durable', None),
                'auto_delete': row.get('auto_delete', None),
                'internal': row.get('internal', None),
                'message_stats': row.get('message_stats', {}),
                'arguments': row.get('arguments', {}),
            })
        return result

    def get_exchange(self, vhost: str, name: str, get_binding: bool = False) -> dict:
        """
        Get an exchange
        :return:
        """
        url = self.get_api_url('exchanges/{vhost}/{name}'.format(vhost=vhost, name=name))
        data = self.make_get_request(url)
        if data is None:
            return {}
        result = json.loads(data)
        if get_binding:
            result['bindings'] = {
                'source': self.get_exchange_binding_source(vhost, name),
                'destination': self.get_exchange_binding_destination(vhost, name),
            }
        return result

    def get_exchange_binding_source(self, vhost: str, name: str) -> list:
        """
        Get list of all bindings in which a given exchange is the source
        :return:
        """
        url = self.get_api_url(
            'exchanges/{vhost}/{name}/bindings/source'.format(vhost=vhost, name=name))
        data = self.make_get_request(url)
        if data is None:
            return []
        result = json.loads(data)
        return result

    def get_exchange_binding_destination(self, vhost: str, name: str) -> list:
        """
        Get list of all bindings in which a given exchange is the destination
        :return:
        """
        url = self.get_api_url(
            'exchanges/{vhost}/{name}/bindings/destination'.format(vhost=vhost, name=name))
        data = self.make_get_request(url)
        if data is None:
            return []
        result = json.loads(data)
        return result

    def get_queues(self, vhost: str = None) -> list:
        """
        Get queues information
        :return:
        """
        if vhost is None:
            url = self.get_api_url('queues')
        else:
            url = self.get_api_url('queues/{vhost}'.format(vhost=vhost))
        data = self.make_get_request(url)
        if data is None:
            return []
        data = json.loads(data)
        result = []
        for row in data:
            result.append({
                'name': row.get('name', None),
                'vhost': row.get('vhost', None),
                'exclusive': row.get('exclusive', None),
                'auto_delete': row.get('auto_delete', None),
                'durable': row.get('durable', None),
                'messages_persistent': row.get('messages_persistent', None),
                'messages_unacknowledged_ram': row.get('messages_unacknowledged_ram', None),
                'messages_ready_ram': row.get('messages_ready_ram', None),
                'messages_ram': row.get('messages_ram', None),
                'consumers': row.get('consumers', 0),
                'state': row.get('state', None),
                'idle_since': row.get('idle_since', None),
                'memory': row.get('memory', None),
                'message_stats': row.get('message_stats', {}),
                'arguments': row.get('arguments', {}),
            })
        return result

    def get_queue(self, vhost: str, name: str, get_binding: bool = False) -> dict:
        """
        Get an queue
        :return:
        """
        url = self.get_api_url('queues/{vhost}/{name}'.format(vhost=vhost, name=name))
        data = self.make_get_request(url)
        if data is None:
            return {}
        data = json.loads(data)
        result = {
            'name': data.get('name', None),
            'vhost': data.get('vhost', None),
            'exclusive': data.get('exclusive', None),
            'auto_delete': data.get('auto_delete', None),
            'durable': data.get('durable', None),
            'messages_persistent': data.get('messages_persistent', None),
            'messages_unacknowledged_ram': data.get('messages_unacknowledged_ram', None),
            'messages_ready_ram': data.get('messages_ready_ram', None),
            'messages_ram': data.get('messages_ram', None),
            'consumers': data.get('consumers', 0),
            'state': data.get('state', None),
            'idle_since': data.get('idle_since', None),
            'memory': data.get('memory', None),
            'message_stats': data.get('message_stats', {}),
            'arguments': data.get('arguments', {}),
        }
        if get_binding:
            result['bindings'] = self.get_queue_binding(vhost, name)

        return result

    def get_queue_binding(self, vhost: str, name: str) -> list:
        """
        Get list of all bindings on a given queue
        :return:
        """
        url = self.get_api_url(
            'queues/{vhost}/{name}/bindings'.format(vhost=vhost, name=name))
        data = self.make_get_request(url)
        if data is None:
            return []
        result = json.loads(data)
        return result


if __name__ == '__main__':
    amqp_uri = 'amqp://teko-dev:9BIdT6KGRFsM23b9j86e@brisk-pigeon.rmq.cloudamqp.com/teko-dev'
    connector = RESTConnector()
    connector.get_parameters_from_amqp_uri(amqp_uri)
    result = connector.get_queue(vhost='teko-dev', name='evt-teko.om-payment.refund.cmd.create--is.sales_invoice.sales_return_invoice', get_binding=True)
    print(json.dumps(result))
    exit(1)
