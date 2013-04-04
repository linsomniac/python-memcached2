#!/usr/bin/env python3


class ServerConnection:
	def __init__(self, uri, timeout=None):
		self.uri = uri
		self.timeout = timeout
		self.reset()

	def reset(self):
		self.buffer = ''
		self.backend = None

	def consume_from_buffer(self, length):
		data = self.buffer[:length]
		self.buffer = self.buffer[length:]
		return data

	def read_until(self, search):
		start = 0
		search_len = len(search)

		while True:
			if self.buffer:
				pos = self.buffer.find(search, start)
				if pos >= 0:
					return self.consume_from_buffer(pos + search_len)
				else:
					start = max(0, len(self.buffer) - search_len)

			raise NotImplementedError('Read from backend')
