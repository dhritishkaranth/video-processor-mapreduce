#!/usr/bin/env python

import sys
import hdfs
import os
import subprocess
import ffmpeg
import logging

namenode_url = "http://ec2-52-3-236-66.compute-1.amazonaws.com:9870"
hdfs_basepath = "/data/"
tmp_dir = "/mnt/tmp/vs/reducer_cache"
chunk_size = 2

logging.basicConfig(filename="/home/hadoop/reduce.log", level=logging.DEBUG)

client = hdfs.InsecureClient(namenode_url)

subprocess.run(["rm", "-rf", tmp_dir])
subprocess.run(["mkdir", "-p", tmp_dir])

files = []

print("starting reducer")

for line in sys.stdin:
        line = line.strip()
        logging.info(f"Input {line}")
        print(line)
        if len(line)  == 0:
                continue
        try:
                filename, _ = line.split('\t')
        except Exception as e:
                print("ERROR when parsing filename")
        source_path = os.path.join(hdfs_basepath, filename)
        #print(f"Copying {filepath} to local.")
        #print(client.status(filepath))
        local_path = os.path.join(tmp_dir, filename)
        #print(f"{source_path} {local_path}")
        subprocess.run(["hdfs", "dfs", "-copyToLocal", source_path, local_path])
        name_parts = filename.split('.')
        output_path = os.path.join(tmp_dir, f"{name_parts[0]}_scale.mp4")
        subprocess.run(["ffmpeg", "-hide_banner", "-loglevel", "panic", "-i", f"{local_path}", "-vf", "scale=480:-1", "-crf", "18", f"{output_path}"])
        subprocess.run(["hdfs", "dfs", "-rm", "-f", os.path.join(hdfs_basepath, f"{name_parts[0]}_scale.mp4")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["hdfs", "dfs", "-copyFromLocal", output_path, os.path.join(hdfs_basepath, f"{name_parts[0]}_scale.mp4")])
        logging.info(f"{filename} processed.")


subprocess.run(["rm", "-rf", tmp_dir])
