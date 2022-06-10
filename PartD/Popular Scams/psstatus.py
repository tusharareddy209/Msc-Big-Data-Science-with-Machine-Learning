from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time as time

class popularscams(MRJob):
	def mapper(self, _, lines):
		line = json.loads(lines)
		keys = line["result"]
		for key in keys:
			record = line["result"][key]
			category = record["category"]
			status=record["status"]
			addresses = record["addresses"]
			for address in addresses:
				yield ((category,status),1)

	def combiner(self, key, value):
		yield (key, sum(value))

	def reducer(self, key, value):
		yield (key, sum(value))

if __name__ == '__main__':
	popularscams.run()
