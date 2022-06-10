from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time as time

class popularscams(MRJob):
	def mapper1(self, _, lines):
		try:
			fields = lines.split(",")
			if len(fields) == 7:
				timestamp = int(fields[6])
				month = time.strftime("%m", time.gmtime(timestamp))
				year = time.strftime("%y", time.gmtime(timestamp))
				yearmonth=(year,month)
				value = float(fields[3])
				address = fields[2]
				yield (address, (value,1,yearmonth))

			else:
				line = json.loads(lines)
				keys = line["result"]
				for key in keys:
					record = line["result"][key]
					category = record["category"]
					addresses = record["addresses"]
					for address in addresses:
						yield (address, (category,2,0))

		except:
			pass

	def reducer1(self, key, elements):
		value=[]
		category=""
		date=[]
		p=False
		for element in elements:
			if element[1] == 2:
				category=element[0]
				p=True
			else:
				if isinstance(element[0],float) and element[2]:
					value.append(element[0])
					date.append(element[2])
		if p:
			for i,j in zip(value,date):
				yield ((j, category),i)


	def mapper2(self,key,value):
		yield(key,value)
	def combiner1(self, key, value):
		yield(key,sum(value))
	def reducer2(self, key, value):
		yield(key,sum(value))

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2,combiner=self.combiner1, reducer = self.reducer2)]

if __name__ == '__main__':
	popularscams.run()
