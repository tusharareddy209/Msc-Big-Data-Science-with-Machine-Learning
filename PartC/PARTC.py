from mrjob.job import MRJob
from mrjob.step import MRStep

class PARTC(MRJob):
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9:
				miner1 = fields[2]
				size1 = fields[4]
				yield (miner1, int(size1))

		except:
			pass

	def reducer1(self, miner1, size1):
		try:
			yield(miner1, sum(size1))

		except:
			pass


	def mapper2(self, miner1, totalsize1):
		try:
			yield(None, (miner1,totalsize1))
		except:
			pass

	def reducer2(self, _, msize1):
		j = 0
		try:
			sortsize1 = sorted(msize1, reverse = True, key = lambda x:x[1])
			for i in sortsize1[:10]:
				yield(i[0],i[1])
		except:
			pass
	

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PARTC.run()	
	
		

