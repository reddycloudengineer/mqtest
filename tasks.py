from celery import Celery
import time
import base64
import pika
from PIL import Image
from io import BytesIO
import boto3
import json
app = Celery('tasks',backend='rpc://', broker='amqp://guest@localhost//')

@app.task
def sendMessage():
    connection=pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel=connection.channel()
    channel.queue_declare(queue='image_queue')
    channel.basic_publish(exchange='',routing_key='hello',body='Hello World!')

    connection.close()
    print("[x] Sent 'Hello world' ")
    return "[x] Sent 'Hello world' "
def callback(ch,method,properties,body):

    body=json.loads(body)
    # print("[x] Received %r from callback " %body['data'][2:len(body['data'])-1])
    warp_id=body['warp_id']
    encodedImage=body['data']
    print('encoded image from callback',encodedImage[0:10])

    imageSaveS3(encodedImage,warp_id)
    print("[x] is done")
    ch.basic_ack(delivery_tag=method.delivery_tag)


@app.task
def consumer():
    connection=pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel=connection.channel()
    channel.basic_consume(callback,queue='image_queue')
    channel.start_consuming()
    print(' [*] Waiting for messages. To exit press ctrl+c ')
    return ' [*] Waiting for messages. To exit press ctrl+c '

@app.task
def publisher(n):
    connection=pika.BlockingConnection(pika.ConnectionParameters('localhost'))

    channel=connection.channel()
    channel.queue_declare(queue='image_queue')
    message={}
    message['warp_id']=n
    message['data_type']="image_plate"
    message['data']=imageEncoder('test.png')
    print('message data',message['data'][0:10])
    # print('json dumps',json.dumps(message))
    channel.basic_publish(exchange='',routing_key='image_queue',body=json.dumps(message))
    print("[x] sent %r " %message)

def imageEncoder(imagePath):
    imageEncoded=None
    with open(imagePath, 'rb') as imgFile:
        imageEncoded = base64.b64encode(imgFile.read()).decode('utf8')
    print('image Encoded from imageencode method',imageEncoded)
    return imageEncoded

def imageSaveS3(encodedImageTxt,warp_id):
    print('encoded image txt',encodedImageTxt[0:10])
    im = Image.open(BytesIO(base64.b64decode(encodedImageTxt)))
    im = im.rotate(180)
    tempFname='decode-'+str(warp_id)+'.png'
    im.save('decode-'+str(warp_id)+'.png')
    client = boto3.client('s3',
    aws_access_key_id='AKIAJV43BBV2PTOEETMA',
    aws_secret_access_key='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')
    client.upload_file(tempFname,'imaget-test',tempFname)
    # client.put_object(Body='test file', Bucket='imaget-test', Key='anotherfilename.txt')

