#!/usr/bin/python

from multiprocessing.pool import ThreadPool
import urllib2
from urllib2 import HTTPError
from urllib2 import URLError

class ContentLengthError (HTTPError):
	pass

class HTTPFetchPool:
	_num_thread = 5
	_retry_thread = 50
	_retry_limit = 10
	_thread_pool = None
	_retry_pool = None
	_timeout = 3
	_retry_timeout = 10
	_retry_sleep = 3

	def __init__ (self, num_thread=5, retry_thread=50, retry_limit=10):
		self._num_thread = num_thread
		self._retry_thread = retry_thread
		self._retry_limit = retry_limit
		
	def start (self):
		self._thread_pool = ThreadPool(self._num_thread)
		self._retry_pool = ThreadPool(self._retry_thread)

	def addAsyncJob (self, url, headers=None, data=None, callback=None, *args,
		**kwargs): 
		kwargs['_fetcher'] = self
		kwargs['_url'] = url
		kwargs['_header'] = headers
		kwargs['_data'] = data
		kwargs['_callback'] = callback
		kwargs['_async'] = True
		return self._thread_pool.apply_async(self.download, args, kwargs
				, self.middleman) 
	
	def addSyncJob (self, url, headers=None, data=None, callback=None, *args,
		**kwargs): 
		kwargs['_fetcher'] = self
		kwargs['_url'] = url
		kwargs['_header'] = headers
		kwargs['_data'] = data
		kwargs['_callback'] = callback
		kwargs['_async'] = False
		try:
			result = self._thread_pool.apply(self.download, args, kwargs)
		except Exception as err:
			result.status = -1
			result.exception = err
			print err.reason
			raise
		return self.middleman(result)
	
	def addRetryJob (self, *args, **kwargs):
		if kwargs['_async']:
			return self._retry_pool.apply_async(self.retry, args, kwargs, self.middleman)
		else:
			return self._retry_pool.apply(self.retry, args, kwargs)
	
	@classmethod
	def middleman (cls, result):
		print "Middleman"
		callback = result.kwargs['_callback']
		if result.status == -1 and result.retry_asyncresult != None:
			return result

		if callback:
			result = callback(result)

		return result

	def stop (self):
		self._thread_pool.close()
		self._thread_pool.join()
		self._retry_pool.close()
		self._retry_pool.join()

	def retry (self, *args, **kwargs):
		url = kwargs['_url']
		headers = kwargs['_header']
		data = kwargs['_data']

		print "Start retry " + url

		result = HTTPFetchResult()
		
		retrycount = 0
		
		while retrycount < self._retry_limit:
			retrycount = retrycount + 1
			try:
				result = doDownload(url, headers, data, self._retry_timeout)
			except (HTTPError, URLError) as e:
				print "Error %d/%d" % (retrycount, self._retry_limit)
				print e.reason
				result.status = -1
				result.exception = e
				result.retry_asyncresult = None
			except Exception as err:
				print "Fatal Error " + url + " " + err.reason
				result.status = -1
				result.exception = err
				result.retry_asyncresult = None
				result.args = args
				result.kwargs = kwargs
				return result
			else:
				result.status = 0
				result.exception = None
				result.retry_asyncresult = None
				break
			time.sleep(_retry_sleep)
		
		if result.status < 0:
			print "Failed after %d Retries: %s" % (self._retry_limit, url)
		result.args = args
		result.kwargs = kwargs
		return result

	def download (self, *args, **kwargs):
		url = kwargs['_url']
		headers = kwargs['_header']
		data = kwargs['_data']

		print "Start download " + url

		result = HTTPFetchResult()
		
		try:
			result = doDownload(url, headers, data, self._timeout)
		except Exception as e:
			print "Moved to Retry Pool"
			result.status = -1
			result.exception = e
			if kwargs['_async']:
				result.retry_asyncresult = self.addRetryJob(*args, **kwargs)
			else:
				result = self.addRetryJob(*args, **kwargs)
		
		result.args = args
		result.kwargs = kwargs
		return result
	
def doDownload (url, headers=None, data=None, timeout = 5):
	result = HTTPFetchResult()
	if data and headers:
		req = urllib2.Request(url, data, headers)
	elif headers:
		req = urllib2.Request(url, headers=headers)
	else:
		req = urllib2.Request(url)

	req_obj = None
	#req = urllib2.Request(url)
	try:
		req_obj = urllib2.urlopen(req, timeout=timeout)
	except Exception as err:
		raise err

	result.status = 0
	result.req_obj = req_obj
	ret_data = req_obj.read()
	result.data = ret_data

	if "Content-Length" in req_obj.headers:
		if len(ret_data) != int(req_obj.headers["Content-Length"]):
			raise ContentLengthError()
	else:
		print "No CL"

	return result
	
class HTTPFetchResult(object):
	args = None
	kwargs = None
	data = None
	status = 0
	req_obj = None
	exception = None
	retry_asyncresult = None

#def test_callback(result):
#	print result.args
#	print "Completed"

#http_pool = HTTPFetchPool()
#http_pool.start()
#http_pool.addAsyncJob("http://www.google.com.hk", callback=test_callback)
#http_pool.addAsyncJob("http://www.google.com", callback=test_callback)
#http_pool.stop()

#class Argument(object):
#	_args = []
#	_kwargs = {}
#
#	def __init__ (self, *args, **kwargs):
#		self._args = args
#		self._kwargs = kwargs

#def test (obj):
#	print obj._kwargs['name']
#	print obj._args[0]
#	return

#def th (*args, **kwargs):
#	print "hihi"
#	tempobj = Argument()
#	tempobj._args = args
#	tempobj._kwargs = kwargs;
#	return tempobj

#threadpool = ThreadPool(5)

#temp = threadpool.apply_async(th, (345,), {'name':"Sashiko"},test)
#obj = temp.get()
#print obj._args[0]

#threadpool.close()
#threadpool.join()

