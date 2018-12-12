# coding =utf-8
import json

from mqconnector.rabbit import RESTConnector

if __name__ == '__main__':
    rest_connector = RESTConnector()
    amqp_uri = 'amqp://username:password@mustang.rmq.cloudamqp.com/vhost'
    print(json.dumps(rest_connector.get_queues(vhost='vhost')))
