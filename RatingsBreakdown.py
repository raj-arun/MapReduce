""" 
Read the data
How many 1 Star, 2 Star, 3 Star, 4 Star and 5 Star movies are there in the data file
"""

# Import MRJob and MRStep
from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):
    def steps(self):
		# Map Reduce Steps
        return [
                MRStep(mapper=self.mapper_get_ratings,
                       reducer=self.reducer_count_ratings)
               ]

    # Define Mapper
	def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1

    # Define Reducer
	def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()
