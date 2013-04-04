#!/usr/bin/env python3


class Memcached2Exception(Exception):
	pass


class UnknownProtocol(Memcached2Exception):
	pass


class InvalidURI(Memcached2Exception):
	pass


class ServerConnection:
	def __init__(self, uri, timeout=None):
		self.uri = uri
		self.parsed_uri = self.parse_uri()
		self.timeout = timeout
		self.backend = None
		self.reset()

	def reset(self):
		self.buffer = b''
		if self.backend:
			self.backend.close()
		self.backend = None

	def consume_from_buffer(self, length):
		data = self.buffer[:length]
		self.buffer = self.buffer[length:]
		return data

	def parse_uri(self):
		import re
		m = re.match(r'memcached://(?P<host>[^:]+)(:(?P<port>[0-9]+))?/',
				self.uri)
		if m:
			group = m.groupdict()
			port = group.get('port')
			if not port:
				port = 11211
			port = int(port)
			return {'protocol': 'memcached', 'host': group.get('host'),
					'port': port}

		raise InvalidURI('Invalid URI: {0}'.format(self.uri))

	def connect(self):
		if self.backend:
			return

		self.reset()
		if self.parsed_uri['protocol'] == 'memcached':
			import socket
			self.backend = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			self.backend.connect((self.parsed_uri['host'],
					self.parsed_uri['port']))
			return

		raise UnknownProtocol('Unknown connection protocol: {0}'
				.format(self.parsed_uri['protocol']))

	def send_command(self, command):
		self.backend.send(command + b'\n')

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

			self.buffer += self.backend.recv(10000)
