# encoding=UTF-8
#!flask/bin/python

import uuid
from flask import Flask, request, json, render_template
from flask.json import jsonify

from ConfigParser import RawConfigParser
from kafka.client import KafkaClient
from kafka import KafkaProducer, TopicPartition, SimpleClient, SimpleConsumer, KafkaClient
from kafka.errors import KafkaError
from kafka.consumer import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import OffsetAndMetadata
import datetime, logging

app = Flask(__name__)

class ExecuteResult:
        code=0
        message=""

class ReceiveDataResult:
        topic_messages=[]
        guid=""
        message=""
        code=0

class SaveDataResult:
        CreateDate=""  #datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        guid=""
        message=""
        code=0
        MsgInfo=[]

class MsgPartitionInfo:
        partition_ID=0
        get_last_offset=0

class routingInfo:
        path=""
        method=""
        description=""

def encode_ExecuteResult(obj):
        if isinstance(obj, ExecuteResult):
                return obj.__dict__
        return obj

def encode_ReceiveDataResult(obj):
        if isinstance(obj, ReceiveDataResult):
                return obj.__dict__
        return obj

def encode_MsgPartitionInfo(obj):
        if isinstance(obj, MsgPartitionInfo):
                return obj.__dict__
        return obj

def encode_SaveDataResult(obj):
        if isinstance(obj, SaveDataResult):
                return obj.__dict__
        return obj

def encode_routingInfo(obj):
        if isinstance(obj, routingInfo):
                return obj.__dict__

# Get broker list from config file
# 解析config檔取得broker ip
def GetConfigBrokers():
        config = RawConfigParser()
        config.read('config.cfg')
        single_section = config.items("bootstrap_servers")
        servers = []
        for item in single_section:
                servers.append(item[1])
        return servers

tmpbootstrap_servers = GetConfigBrokers()
kafkaclient = KafkaClient(tmpbootstrap_servers)

# Logging
logging.basicConfig(level=logging.DEBUG)
logging.debug('Debug...')

@app.route('/help', methods = ['GET'])
def help():
        """列出所有的routing及說明"""
        func_list = []

        htmlcode = "<h3>routing list</h3>"
        for rule in app.url_map.iter_rules():
                if rule.endpoint != 'static':

                        info = routingInfo()
                        description = str(app.view_functions[rule.endpoint].__doc__)
                        info.description = description.decode('utf-8')
                        info.path = rule.rule

                        httpMethods = str(rule.methods)
                        if 'GET' in httpMethods:
                                index = '[GET]' + str(rule.rule)
                                info.method='GET'
                        else:
                                index = '[POST]' + str(rule.rule)
                                info.method='POST'
                        htmlcode = htmlcode + "<B><font color='blue'>METHOD</font></B>: "+ info.method +"<BR>"
                        htmlcode = htmlcode + "<B><font color='green'>  PATH  </font></B>: " + info.path + "<BR>"
                        htmlcode = htmlcode + "<B><font color='green'>   DESC  </font></B>: " + info.description + "<BR><BR>"

                        info2 = json.dumps(info, default=encode_routingInfo, ensure_ascii=False)

                        func_list.append(info2)
        htmlcode = htmlcode + "<hr><a href='index'>Back to index</a><BR><a href='add_topic'>go to add topic</a>"

        return htmlcode
        #return json.dumps(func_list, ensure_ascii=False)

# Get方法
# 回傳hello訊息e
@app.route('/', methods=['GET'])
@app.route('/index', methods=['GET'])
def index():
        """ 程式說明及步驟"""
        return render_template('index.html')


# Get方法顯示add_topic.html頁面，供使用前填寫資料
@app.route('/add_topic', methods=['GET'])
def add_topic_html():
        """顯示add_topic.html頁面，供使用前填寫資料"""
        return render_template('add_topic.html')

# submit，取得使用者資料並建立topic
@app.route('/add_topic', methods=['POST'])
def submit_add_topic():
        """在add_topic頁面按下submit後的動作，取得使用者填寫的資料並建立topic"""
        try:
                message="<h3>Result</h3>"
                topic = request.form.get("input_topic")

                print(topic)
                producer = KafkaProducer(bootstrap_servers=tmpbootstrap_servers)
                par = producer.partitions_for(topic)
                par = producer.partitions_for(topic+'_error_msg')
                par = producer.partitions_for(topic+'_error_msg_log')
                producer.flush()

                group = topic + '-consumer'

                consumer2 = KafkaConsumer(bootstrap_servers=tmpbootstrap_servers, enable_auto_commit=False, group_id=group)
                try:
                        consumer2.subscribe([topic,topic+'_error_msg',topic+'_error_msg_log'])
                except Exception as ex:
                        print('error when create consumer2.....')
                        print(str(ex))
                        consumer2.close()

                consumer2.close()
                message = message + "<h4>Success to add 3 topics!</B></h4>"
                message = message + "1. " + topic + "<BR>"
                message = message + "2. " + topic + "_error_msg<BR>"
                message = message + "3. " + topic + "_error_msg_log<BR>"

        except Exception as e:
                message = message + "<B>Fail to add topic : " + topic + "<B><BR>Detail:"
                message = message + str(e)

                message = message + "<hr><a href='index'>Back to index</a><BR>"
        message = message + "<a href='help'>help</a>"

        return message

# Input kafka message into topic with json, json data includes "input_data" & "topic"
# broker list is in config file
# 傳入topic name, data，將data塞入topic
@app.route('/input_error_msg', methods=['POST'])
def AddMessage():
        """API: 呼叫時傳入topic name, input_data 2個屬性，並將input_data塞入topic"""
        api_message = ExecuteResult()
        api_statuscode = 200
        try:
                input_data = request.json['input_data'].encode('utf-8')
                topic = request.json['topic'].encode('utf-8')+'_error_msg'
                print(input_data)

                if not (CheckTopicExsited(topic)):
                        api_message.message="Topic ("+ topic +") cannot be found! It may have not been created."
                        api_message.code=100
                else:
                        producer = KafkaProducer(bootstrap_servers=tmpbootstrap_servers)
                        producer.send(topic, input_data)
                        producer.flush()
                        api_message.message="ok"
                        api_message.code=0

        except Exception as e:
                api_message.message = str(e)
                api_statuscode=500
        finally:
                return json.dumps(api_message, default=encode_ExecuteResult), api_statuscode


# Get un-committed data from topic
# 取得未committed的資料
@app.route('/receive_error_msg', methods=['POST'])
def Consuming():
        """API: 呼叫時需提供group, topic，API會回傳目前 "尚未committed" 的資料"""
        api_statuscode=200
        result = ReceiveDataResult()
        try:
                group = request.json['group'].encode('utf-8')
                topic = request.json['topic'].encode('utf-8')+'_error_msg'

                if not (CheckTopicExsited(topic)):
                        result.message="Topic ("+ topic +") cannot be found! It may have not been created."
                        result.code=100
                else:
                        result = getMsgData(topic, group, result)

        except Exception as e:
                result.message = str(e)
                api_statuscode=500

        finally:
                return json.dumps(result, default=encode_ReceiveDataResult),api_statuscode


@app.route('/commit_error_msg', methods=['POST'])
def CommitData():

        """API: 呼叫時提供topic, guid, group，API會依照guid查詢當初取資料時的last offset，並依last offset 對 topic 進行 Commit"""
        api_message=""
        api_statuscode=200
        try:

                original_topic = request.json['topic'].encode('utf-8')+'_error_msg'
                topic = request.json['topic'].encode('utf-8')+'_error_msg_log'
                guid = request.json['guid'].encode('utf-8')
                group = request.json['group'].encode('utf-8')
                print(topic)
                print(guid)
                print(group)

                if not (CheckTopicExsited(topic)):
                        return "Topic cannot be found! This may have not been created.",200

                client3 = KafkaClient(tmpbootstrap_servers)
                simplecon = SimpleConsumer(client3, group, topic, auto_commit=False)
                simplecon_messages = simplecon.get_messages(count=20)

                for msg in simplecon_messages:
                        msgGuid = getMsgDataGuid(msg.message.value)
                        if msgGuid == guid:
                                msgInfos2 = get_last_offset_data(msg.message.value)
                                msgInfos = json.loads(msgInfos2)
                                for offset_data in msgInfos:
                                        commitTopic(original_topic, group, int(offset_data['partition_ID']), int(offset_data['get_last_offset']))
                api_message="ok"

        except Exception as e:
                api_message=str(e)
                api_statuscode=500

        finally:
                return api_message,api_statuscode

def commitTopic(topic, group, partition, commit_offset):
        try:
                print("[commitTopic]\n")
                print("topic="+topic + ", group=" + group + ", partition=" + str(partition) + ", commit_offset="+str(commit_offset))
                consumer2 = KafkaConsumer(bootstrap_servers=tmpbootstrap_servers, enable_auto_commit=False, group_id=group)
                tp = TopicPartition(topic, partition)

                if int(commit_offset) > 0:

                        consumer2.commit({
                                tp: OffsetAndMetadata(commit_offset, None)
                                })

        except Exception as e:
                print(e)
                print('error when commitTopic')
        finally:
                print('commitTopic end')

def getMsgData(topic, group, result):
        try:

                saveResult = SaveDataResult()
                saveResult.guid = str(uuid.uuid4())
                saveResult.CreateDate = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                msgInfos =[]
                result.guid = saveResult.guid
                result.topic_messages=[]

                consumer = KafkaConsumer(bootstrap_servers=tmpbootstrap_servers, enable_auto_commit=False, group_id=group)

                # Get all partitions by topic
                par = consumer.partitions_for_topic(topic)

                for p in par:
                        tp = TopicPartition(topic, p)
                        consumer.assign([tp])
                        print(tp)
                        info = MsgPartitionInfo()

                        # Get committed offset
                        print('start to get committed offset.....')
                        try:
                                committed = consumer.committed(tp) or 0
                        except Exception, e_commit:
                                print(str(e_commit))

                        # Move consumer to end to get the last position
                        consumer.seek_to_end(tp)
                        last_offset = consumer.position(tp)

                        # Move consumer to beginning to get the first position
                        consumer.seek_to_beginning()
                        now_offset = consumer.position(tp)
                        from_offset = committed

                        if from_offset is None:
                                from_offset=now_offset

                        if from_offset < now_offset:
                                from_offset=now_offset

                        info.partition_ID = tp.partition
                        info.get_last_offset = last_offset
                        msgInfos.append(info)

                        print("[%s] partition(%s) -> now:%s,  last:%s,  committed:%s" % (tp.topic, tp.partition, now_offset, last_offset, committed))

                        # Get msg from position to offset
                        while from_offset < last_offset:
                                        consumer.seek(tp, from_offset)
                                        polldata=consumer.poll(100)
                                        from_offset += 1
                                        result.topic_messages.append(polldata[tp][0].value)

                saveResult.MsgInfo = json.dumps(msgInfos, default=encode_MsgPartitionInfo)
                print(saveResult.MsgInfo)
                consumer.close()
                saveResult.message="Success"
                saveResult.Code=200

                producer = KafkaProducer(bootstrap_servers=tmpbootstrap_servers)
                producer.send(topic+"_log", json.dumps(saveResult, default=encode_SaveDataResult))
                producer.flush()                

        except Exception as e:
                result.message=str(e)
                result.code=500
        finally:
                result.code=200
                return result


def getMsgDataGuid(msgData):
        python_obj = json.loads(msgData)
        msg_guid = python_obj["guid"]
        return python_obj["guid"]

def get_last_offset_data(offsetData):
        python_obj = json.loads(offsetData)
        infos = python_obj["MsgInfo"]
        return infos

def CheckTopicExsited(topic):

        consumer = KafkaConsumer(bootstrap_servers=tmpbootstrap_servers, enable_auto_commit=False, group_id='consumer')

        # Get all partitions by topic
        par = consumer.partitions_for_topic(topic)
        print(par)

        if par is None:
                return False
        return True

if __name__ ==  '__main__':
        app.run(debug=True, host='0.0.0.0', port=50001)
