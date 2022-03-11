import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from py spark.sql import Row, SQLContext
import os.path
from os import path
import json
import urllib
import urllib.request

if os.path.isdir('C:/Users/Rishabh/Desktop/twitter') != True:
    try:
        os.mkdir('C:/Users/Rishabh/Desktop/twitter')
        print("Twitter folder created......")
    except Exception as e:
        print(e)

def process(rdd):
    pd = "C:/Users/Rishabh/Desktop/twitter"
    data = rdd.collect()
    for d in data:
        x=json.loads(d)

        #text, created_at, username
        if "text" in x:
            create_dir(pd, "text")
            tdir = os.path.join(pd, "text")
            s = x['text'].replace('\n', ' ') + ' created_at: ' + x['created_at'] + ' username: ' + x['user_name']
            write_into_file(s,tdir, "text_file.txt")

        #images
        if "image_url" in x:
            create_dir(pd, "images")
            imdir = os.path.join(pd, "images")
            write_into_file(x['image_url'], imdir, "image_url_file.txt")
            print(x['image_url'], end='\n\n')
            download(x['image_url'], imdir, "Image")

        #videos
        if "video_url" in x:
            create_dir(pd, "videos")
            vdir = os.path.join(pd, "videos")
            write_into_file(x['video_url'], vdir, "video_url_file.txt")
            print(x['video_url'], end='\n\n')
            download(x['video_url'], vdir, "Video")

#creating folder
def create_dir(pd, directory):
    path = os.path.join(pd, directory)

    if not os.path.isdir(path):
        try:
            os.mkdir(path)
            print (path, "folder created...........")
        except Exception as e:
            print(e)
    else:
        pass

#writing data in file
def write_into_file(y, directory, fname):
    path = os.path.join(directory, fname)
    with open(path, "a") as fp:
        fp.write(str(y))
        fp.write('\n')

#downloading images or videos
def download (url, path, n):
    if n=="Image" :
        fp = os.path.join(path, url.split('/')[4])
    else:
        if "mp4" in str(url):
            fp = os. path.join(path, url.split('/')[4] + '.mp4')

    try:
        urllib.request.urlretrieve(url, fp)
        print(n, 'downloaded and saved as : ', fp, end = '\n---------------\n\n')
    except Exception as e:\
        print(e)

sc = SparkContext(master="local[*]", appName="tweetConsumer2")
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)
ssc.checkpoint("checkpoint_TwitterApp")
dataStream = ssc.socketTextStream("localhost", 9009)

lines = dataStream.window(10,10)
lines.pprint()
lines.foreachRDD(process)
# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()