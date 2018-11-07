""" 
Read the data
Find out how many times a movie was rated
"""
# Import MRJob and MRStep 
from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieBreakdown(MRJob):
    def steps(self):
		# Map Reduce Steps
        return [MRStep(mapper=self.mapper_get_movies,
                       reducer=self.reducer_count_movies
                       )]

	# Define Mapper
    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
		
	# Define Reducer
    def reducer_count_movies(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MovieBreakdown.run()
