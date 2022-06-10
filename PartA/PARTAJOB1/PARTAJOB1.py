from mrjob.job import MRJob
import re
import time
import statistics

class PARTA(MRJob):
	
	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				timemy = int(fields[6])
				month1 = time.strftime("%m", time.gmtime(timemy))
				year1 = time.strftime("%y", time.gmtime(timemy))
				yield ((month1,year1), 1)
		except:
			pass

	def reducer(self,word,counts):
		yield(word,sum(counts))

if __name__ == '__main__':
	PARTA.run()
