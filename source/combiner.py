#!/usr/bin/env python

import sys
import os
import subprocess
import logging

hdfs_basepath = "/data/video2"
tmp_dir = "/mnt/tmp/vs/combine_cache/"
chunk_size = 4

logging.basicConfig(filename='/home/hadoop/combine.log', level=logging.DEBUG)

subprocess.call(["rm", "-rf", tmp_dir])
subprocess.call(["mkdir", "-p", tmp_dir])

files = []

fnull = open(os.devnull, 'w')

logging.info("Starting combiner")

subprocess.call(["hdfs", "dfs", "-copyToLocal", os.path.join(hdfs_basepath, "*_scale.mp4"), tmp_dir])

for file in os.listdir(tmp_dir):
        if '_scale.mp4' in file:
                files.append(file)


with open(os.path.join(tmp_dir, 'list.txt'), 'w') as file_handle:
        for file in sorted(files):
                file_handle.write("file '" + os.path.join(tmp_dir, file) + "'\n")
outputfile = os.path.join(tmp_dir, "output.mp4")
subprocess.call(["ffmpeg", "-f", "concat", "-safe", "0", "-i", os.path.join(tmp_dir, "list.txt"), "-c", "copy", outputfile])

hdfs_target = os.path.join(hdfs_basepath, "output.mp4")
subprocess.call(['hdfs', 'dfs', '-rm', '-f', hdfs_target], stderr=fnull, stdout=fnull)
subprocess.call(["hdfs", "dfs", "-copyFromLocal", outputfile, hdfs_target])

subprocess.call(["rm", "-rf", tmp_dir])
logging.debug("combiner done")
