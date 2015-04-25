#!/usr/bin/python

import logging
import argparse
import requests
import m3u8
from urlparse import urljoin
import grequests
from Crypto.Cipher import AES
from threading import Lock
from time import sleep

#setting
tail_size = 5
pool_size = 10

logger = logging.getLogger("HLS Downloader")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logf = logging.FileHandler("hls.log")
logf.setLevel(logging.DEBUG)
logf.setFormatter(formatter)

logger.addHandler(logf)

parser = argparse.ArgumentParser(description='Crawl a HLS Playlist')
parser.add_argument('url', type=str, help='Playlist URL')
parser.add_argument('--file', type=str, help='Output File')
parser.add_argument('-t', '--tail', action="store_true", help='Tail Mode')
args = parser.parse_args()

playlist_url = args.url
logger.info("Playlist URL: " + playlist_url)

control = requests.Session()
data = requests.Session()
data_pool = grequests.Pool(pool_size)

# Tail Mode
if args.tail:
	tail_mode = True
else:
	tail_mode = False

# File Mode
if args.file:
	file_mode = True
	out_file = args.file
else:
	file_mode = False


# Get Main Playlist

mpl_res = control.get(playlist_url)
content = mpl_res.content

# Detect Resolution
variant_m3u8 = m3u8.loads(content)

streams_uri = dict()
for playlist in variant_m3u8.playlists:
	if playlist.stream_info.resolution :
		resolution = int(playlist.stream_info.resolution[1])
		logger.info("Stream at %dp detected!" % resolution)
	else:
		resolution = int(playlist.stream_info.bandwidth)
		logger.info("Stream with bandwidth %d detected!" % resolution)

	streams_uri[resolution] = urljoin(playlist_url, playlist.uri)

# playlist.uri

auto_highest = True
stream_res = 0
# Pick Stream (Resolution)
if auto_highest and len(variant_m3u8.playlists) > 0:
	stream_res = max(streams_uri)

	logger.info("Stream Picked: %dp" % stream_res)

	stream_uri = streams_uri[stream_res]
else:
	stream_uri = playlist_url
logger.info("Chunk List: %s" % (stream_uri))
# for stream, uri in streams_uri.iteritems():

old_start = -1
old_end = -1
new_start = -1
new_end = -1

chunk_retry_limit = 10
chunk_retry = 0
chunk_retry_time = 10

last_write = -1

if file_mode:
	out_f = open(out_file, "wb")
	out_f_lock = Lock()
	fetched_set = set()
	fetched_data = dict()

error_count = {}

while True:
	# Get Playlist

	pl_res = control.get(stream_uri)
	if not pl_res.status_code == requests.codes.ok:
		logger.info("Cannot Get Chunklist")
		if chunk_retry < chunk_retry_limit:
			sleep(chunk_retry_time)
			chunk_retry += 1
		else:
			break
	
	content = pl_res.content
	chunklist = m3u8.loads(content)

	# Check Key
	enc = chunklist.key

	if chunklist.key:
		logger.info("Stream Encrypted with %s!", enc.method)
		enc.key = control.get(enc.uri).content

	target_dur = chunklist.target_duration
	start_seq = chunklist.media_sequence

	if last_write == -1:
		last_write = start_seq - 1
	
	seq = start_seq

	seg_urls = dict()

	sleep_dur = 0
	updated = False

	list_end = chunklist.is_endlist

	for segment in chunklist.segments:
	#	print dir(segment)
	#	print segment.uri
	#	print segment.duration
		seg_urls[seq] = urljoin(playlist_url, segment.uri)
		sleep_dur = segment.duration
		seq = seq + 1

	old_start = new_start
	old_end = new_end
	new_start = start_seq
	new_end = seq - 1

	# TODO: Loop Back
	if old_end == -1:
		if tail_mode:
			new_start = new_end - tail_size
			if new_start < start_seq:
				new_start = start_seq				
		else:
			new_start = start_seq
		last_write = new_start - 1
	else:
		new_start = old_end + 1
	
	segment_reqs = list()

	def decode_and_write(resp, seq, enc):
		global error_count
		if resp.status_code != 200 or int(resp.headers['content-length']) != len(resp.content):
			logger.info("Content Problem, Retrying for %d" % (seq))
			req = grequests.get(seg_urls[seq], callback=set_seq_hook(seq, enc), session=data)
			error_count[seq] = error_count[seq] + 1
			if error_count[seq] > 10:
				logger.warning("Seq %d Failed" % (seq))
				return
			sleep(3)
			grequests.send(req, data_pool)
			return

		logger.info("Processing Segment #%d" % (seq))
		out_data = resp.content
		if enc:
			if not enc.iv:
				enc.iv = "0000000000000000"
			dec = AES.new(enc.key, AES.MODE_CBC,enc.iv)
			out_data = dec.decrypt(out_data)

		if file_mode:
			global last_write
			global fetched_set
			global fetched_data
			out_f_lock.acquire()
			fetched_set.add(seq)
			fetched_data[seq] = out_data

			while True:
				if last_write + 1 in fetched_set:
					last_write = last_write + 1
					write_data = fetched_data[last_write]
					logger.debug("Writing %d to %s" % (last_write, out_file));
					out_f.write(write_data)
					del fetched_data[last_write]
				else:
					break
			out_f_lock.release()


		else:
			filename = str(seq) + ".ts"
			logger.debug("Write to %s" % (filename))
			video_f = open(filename, "wb")
			video_f.write(out_data)
			video_f.close()
				

	def set_seq_hook(seq, enc):
		def hook(resp, **data):
			decode_and_write(resp, seq, enc)
			return None
		return hook

	for seq in range(new_start, new_end + 1):
		req = grequests.get(seg_urls[seq], callback=set_seq_hook(seq, enc), session=data)
	#	segment_reqs.append(req)
		error_count[seq] = 0
		grequests.send(req, data_pool)
		updated = True


	#grequests.send(segment_reqs, data_pool)

#	if not updated:
	sleep_dur = target_dur / 2

	if list_end:
		break

	logger.debug("Sleep for %d secs before reloading" % (sleep_dur))
	sleep(sleep_dur)

logger.info("Stream Ended")
data_pool.join()

if file_mode:
	out_f.close()
