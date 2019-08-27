import pika
from impala.dbapi import connect
from sshtunnel import SSHTunnelForwarder
import boto
from boto.s3.key import Key
import json
import sys
import smtplib
import logging
from kazoo.client import KazooClient
import base64
import traceback
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQL Statement
SQL_CASCADE = """
    SELECT
        profileid,
        CONCAT(FROM_UNIXTIME(fluentd_timestamp, 'yyyy-MM-dd HH:mm:ss'), ' UTC') AS location_ping_time,
        lat,
        lon
    FROM g3.location_parquet
    WHERE
        ds BETWEEN FROM_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 120), 'yyyy-MM-dd') 
        AND FROM_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd')
        AND profileid = {}
    GROUP BY
        profileid,
        FROM_UNIXTIME(fluentd_timestamp, 'yyyy-MM-dd HH:mm:ss'),
        lat,
        lon
    ORDER BY
        profileid,
        location_ping_time ASC
"""

SQL_LAST_LOCATION = """
    SELECT
        profileid,
        CONCAT(FROM_UNIXTIME(fluentd_timestamp, 'yyyy-MM-dd HH:mm:ss'), ' UTC'),
        lat,
        lon
    FROM g3.location_parquet
    WHERE profileid = {} AND
        ds BETWEEN FROM_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 15), 'yyyy-MM-dd') AND
              FROM_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd')
    ORDER BY fluentd_timestamp DESC
    LIMIT 1
"""

SQL_IP_HISTORY = """
    SELECT
        profileid AS profile_id,
        MAX(CONCAT(FROM_UNIXTIME(fluentd_timestamp, 'yyyy-MM-dd HH:mm:ss'), ' UTC')) AS time,
        ipaddress
    FROM g3.httptx_parquet
    WHERE profileid = '{}'
      AND ds BETWEEN FROM_TIMESTAMP(DATE_SUB(CURRENT_TIMESTAMP(), 8), 'yyyy-MM-dd')
        AND FROM_TIMESTAMP(CURRENT_TIMESTAMP(), 'yyyy-MM-dd')
    group by ipaddress, profileid
    ORDER BY time
"""

def sendAlertMail(msg):
    smtp_obj = smtplib.SMTP(host='smtp.gmail.com', port=587)
    smtp_obj.ehlo()
    smtp_obj.starttls()
    smtp_obj.login(alert_email, alert_passwd)
    smtp_obj.sendmail(alert_email, "eggsy.tsai@grindr.com", msg=msg)

def convert_to_csv(results: list) -> str:
    if len(results) > 0 and len(results[0]) == 4:
        # Add the header
        results.insert(0, ["profile_id", "timestamp", "lat", "lon"])
        # Convert to String"
        return "\n".join(["{}, {}, {}, {}".format(result[0], result[1], result[2], result[3]) for result in results])
    elif len(results) > 0 and len(results[0]) == 3:
        # Add the header
        results.insert(0, ["profile_id", "timestamp", "ip"])
        # Convert to String"
        return "\n".join(["{}, {}, {}".format(result[0], result[1], result[2]) for result in results])

def upload_to_s3(id, result_cascade: list, result_location: list, result_ip: list):
    conn = boto.connect_s3()
    logger.info("Connect to S3")
    bucket = conn.get_bucket(info["AWS"]["bucket_name"])
    key = Key(bucket)
    key.key = info["AWS"]["bucket_key"]+'/{}/cascade_calls.csv'.format(id)
    if len(result_cascade) == 0:
        key.set_contents_from_string("")
    else:
        key.set_contents_from_string(convert_to_csv(result_cascade))

    key.key = info["AWS"]["bucket_key"]+'/{}/last_location.csv'.format(id)
    if len(result_location) == 0:
        key.set_contents_from_string("")
    else:
        key.set_contents_from_string(convert_to_csv(result_location))

    key.key = info["AWS"]["bucket_key"]+'/{}/ip_history.csv'.format(id)
    if len(result_ip) == 0:
        key.set_contents_from_string("")
    else:
        key.set_contents_from_string(convert_to_csv(result_ip))
    logger.info("Put result into S3")

def getIniFromZookeeper(host):
    zk = PyZooConn(host)

    # access_key = bytes.decode(zk.get_data("/config/aws:upload:access_key")[0])
    # secret_access_key = bytes.decode(zk.get_data("/config/aws:upload:secret_key")[0])
    ini = zk.get_data("/config/"+source+":gdpr:consume_service:ini")[0]
    if source == 'legacy':
        request_queue = bytes.decode(zk.get_data("/config/gdpr:location:request:queue_name")[0])
        response_queue = bytes.decode(zk.get_data("/config/gdpr:location:response:queue_name")[0])
    else:
        request_queue = "neo_gdpr.request"
        response_queue = "gdpr_neo.response"
    result = json.loads(ini)
    # result["AWS"]["access_key"] = access_key
    # result["AWS"]["secret_key"] = secret_access_key
    result["RabbitMQ"]["src_queue"] = request_queue
    result["RabbitMQ"]["dest_queue"] = response_queue
    result["EMail"]["passwd"] = bytes.decode(base64.b64decode(result["EMail"]["passwd"]))
    logger.info(result)
    return result

def getIniFromS3(access_key_id, secret_access_key, bucket_name, key_name):
    conn = boto.connect.connect_s3()
    bucket = conn.get_bucket(bucket_name)
    key = Key(bucket)
    key.key = key_name
    return key.get_contents_as_string()

def switchToTunnel(tunnel_host, tunnel_port, tunnel_user, key_dir, local_host, local_port, remote_host="localhost",
                   remote_port=22):
    server = SSHTunnelForwarder(
        ssh_address_or_host=(tunnel_host, tunnel_port),
        ssh_username=tunnel_user,
        host_pkey_directories=[key_dir],
        local_bind_address=(local_host, local_port),
        remote_bind_address=(remote_host, remote_port),
    )
    server.start()

def queryFromImpala(profile_id):
    conn_impala = connect(host=info["Impala"]["host"], port=int(info["Impala"]["port"]),
                          user=info["Impala"]["user"])
    logger.info("Connect to Impala")
    cursor = conn_impala.cursor()

    cursor.execute(SQL_CASCADE.format(profile_id))
    results_cascade = cursor.fetchall()
    logger.info(results_cascade)

    cursor.execute(SQL_LAST_LOCATION.format(profile_id))
    results_location = cursor.fetchall()
    logger.info(results_location)

    cursor.execute(SQL_IP_HISTORY.format(profile_id))
    results_ip = cursor.fetchall()
    logger.info(results_ip)
    return results_cascade, results_location, results_ip

# custom callback function -->
def consume_api(ch, method, properties, body: bytes):
    logging.info("Received %r" % body)
    try:
        body = json.loads(bytes.decode(body))
        uId = body["id"]
        profile_id = body["profileId"]

        if info["Impala"]["is_open"] == "True":
            results_cascade, results_location, results_ip = queryFromImpala(profile_id)
            upload_to_s3(uId, results_cascade, results_location, results_ip)
        else:
            upload_to_s3(uId, [], [], [])

        rmq = RabbitMQ_Util(info["RabbitMQ"]["dest_user"], info["RabbitMQ"]["dest_passwd"],
                            info["RabbitMQ"]["dest_host"], info["RabbitMQ"]["dest_port"],
                            info["RabbitMQ"]["dest_queue"], info["RabbitMQ"]["dest_virtual_host"])

        rmq.produceToRabbitMQ(str(uId))
        rmq.close()

        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Ack response complete")

    except Exception as e:
        logger.info(traceback.format_exc())
        sendAlertMail(str(body)+"\n"+str(traceback.format_exc()))

class PyZooConn:
    def __init__(self, host):
        self.zk = KazooClient(hosts=host)
        self.zk.start()

    def get_data(self, param):
        result = self.zk.get(param)
        print(result)
        return result

    def exist(self, path):
        return self.zk.exists(path)

    def create_node(self, node, value):
        self.zk.create(node, value)

    def delete_node(self, path):
        self.zk.delete(path)

    def close(self):
        self.zk.stop()

class RabbitMQ_Util:
    def __init__(self, user, passwd, host, port, queue_name, virtual_host="/"):
        self._user = user
        self._passwd = passwd
        self._host = host
        self._port = port
        self._queue = queue_name
        self._virtual_host = virtual_host
        self._conn = self._connectToRabbitMQ()

    def _connectToRabbitMQ(self):
        credentials = pika.PlainCredentials(self._user, self._passwd)
        params = pika.ConnectionParameters(host=self._host, port=self._port, virtual_host=self._virtual_host,
                                           credentials=credentials, socket_timeout=600)
        conn = pika.BlockingConnection(params)
        logger.info("Connect to RabbitMQ success !")
        return conn

    def consumeFromRabbitMQ(self):
        channel = self._conn.channel()
        channel.basic_consume(on_message_callback=consume_api, queue=self._queue, auto_ack=False)
        logger.info('Waiting for messages.')
        channel.start_consuming()

    def produceToRabbitMQ(self, id):
        channel = self._conn.channel()
        # channel.queue_declare(self._queue)
        result = '{"id":'+id+', "objectKeys":["location/'+id+'/cascade_calls.csv", "location/'+id+'/last_location.csv"' \
                 ', "location/'+id+'/ip_history.csv"]}'
        channel.basic_publish(exchange='', routing_key=self._queue, body=result)
        logger.info("Sent Message to RMQ Finished !!")
    def close(self):
        self._conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 5:
        logger.info("Parameter not match!!")
        exit()

    zk_host = sys.argv[1]
    alert_email = sys.argv[2]
    alert_passwd = sys.argv[3]
    source = sys.argv[4]

    total_error = 0

    try:
        while total_error < 5:
            try:
                info = getIniFromZookeeper(zk_host)

                # switchToTunnel("spinaltap-dsci.grindr.io", 22, "eggsy.tsai", "~/.ssh",
                #                "localhost", 21050, "cloudera-impala-node-1.dsci.grindr.io", 21050)

                rmq = RabbitMQ_Util(info["RabbitMQ"]["src_user"], info["RabbitMQ"]["src_passwd"], info["RabbitMQ"]["src_host"],
                                    info["RabbitMQ"]["src_port"], info["RabbitMQ"]["src_queue"],
                                    info["RabbitMQ"]["src_virtual_host"])

                rmq.consumeFromRabbitMQ()
            except Exception as e:
                total_error += 1
                sendAlertMail("{}, Prod - {}, retry {}".format(traceback.format_exc(), source, total_error))
                time.sleep(300)
    finally:
        sendAlertMail("Prod GDPR - {} is closed !".format(source))




