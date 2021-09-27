#!/usr/bin/env python

import sys
import os
import subprocess
import logging

hdfs_basepath = "/data/video2"
tmp_dir = "/mnt/tmp/vs/map_cache/"
chunk_size = 4

logging.basicConfig(filename='/home/hadoop/map.log', level=logging.DEBUG)

subprocess.call(["rm", "-rf", tmp_dir])
subprocess.call(["mkdir", "-p", tmp_dir])

files = []

fnull = open(os.devnull, 'w')

logging.info("Starting mapper")

for line in sys.stdin:
        line = line.strip()
        if len(line)  == 0:
                continue
        logging.info("Received input " + line)
        filepath = os.path.join(hdfs_basepath, line)
        subprocess.call(["hdfs", "dfs", "-copyToLocal", filepath, os.path.join(tmp_dir, line)])
        files.append(os.path.join(tmp_dir, line))

for file in files:
        logging.info("Probing " + file)
        probe_res = subprocess.check_output(['ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file])
        duration = float(probe_res)
        logging.info("Duration is " + str(duration))
        chunk_times = []
        step = duration / chunk_size
        for i in range(chunk_size):
                chunk_times.append((step * i, step * (i+1)))
        for i, chunk in enumerate(chunk_times):
                outputfile = os.path.join(tmp_dir, str(i) + ".mp4")
                hdfs_target = os.path.join(hdfs_basepath, str(i) + ".mp4")
                logging.info("Running ffmpeg")
                subprocess.call(["ffmpeg", "-hide_banner", "-loglevel", "panic", "-ss", str(chunk[0]), "-i", file, "-t", str(chunk[1] - chunk[0]), "-c", "copy", outputfile])
                subprocess.call(['hdfs', 'dfs', '-rm', '-f', hdfs_target], stderr=fnull, stdout=fnull)
                logging.debug("Copying to HDFS")
                subprocess.call(["hdfs", "dfs", "-copyFromLocal", outputfile, hdfs_target])
                print( str(i) + ".mp4\t1")
                logging.info("Wrote file " + str(i) + ".mp4 to HDFS.")

subprocess.call(["rm", "-rf", tmp_dir])
logging.debug("mapper done")
