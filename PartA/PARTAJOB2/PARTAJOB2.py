from mrjob.job import MRJob
import re 
import time
import statistics
class PartA2(MRJob):

	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:
				timef = int(fields[6])
				value = float(fields[3])
				months = time.strftime("%m", time.gmtime(timef))
				years = time.strftime("%y", time.gmtime(timef))
				yield((months,years),value)
		except:
			pass
	def combiner(self, date, price):
		sum = 0
		count = 0
		for p in price:
			sum += p 
			count = count +1
		yield(date, (sum, count))

	def reducer(self,date,price):     
		sum = 0
		count = 0
		for p in price:
			sum+=p[0]
			count+=p[1]
		avg = sum/count
		yield(date,avg)


if __name__ == '__main__':
	PartA2.run()
