from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class popularscams(MRJob):
	def mapper1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				address = fields[2]
				value = float(fields[3])
				yield (address, (value,1,1))

			else:
				line = json.loads(lines)
				keys = line["result"]
				for key in keys:
					record = line["result"][key]
					category = record["category"]
					addresses = record["addresses"]
					status=record["status"]
					for address in addresses:
						yield (address, (category,status,2))

		except:
			pass

	def reducer1(self, key, elements):
		value=0
		category=None

		for element in elements:
			if element[2] == 1:
				value = value + element[0]
			else:
				category = element[0]
				status = element[1]
		if category is not None:
			yield ((category,status), value)

	def mapper2(self,key,value):
		yield(key,value)
	def combiner(self, key, value):
		yield(key,sum(value))
	def reducer2(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2,combiner=self.combiner, reducer = self.reducer2)]

if __name__ == '__main__':
	popularscams.run()
