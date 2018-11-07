""" 
Read the data
Find out the movie and the number of times it was rated
Sort the output by Movie Rating in the Ascending Order
"""
# Import MRJob and MRStep
from mrjob.job import MRJob
from mrjob.step import MRStep


class MovieSorted(MRJob):
    def steps(self):
		# Map Reduce Steps. This one has two steps
        return[MRStep(mapper=self.mapper_get_movies,
                      reducer=self.reducer_count_movies
                     ),
               MRStep(reducer=self.reducer_sort_movies)]

	# Define Mapper
    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    # Define Reducer 1
	def reducer_count_movies(self, movie, watched):
        yield str(sum(watched)).zfill(5), movie

    # Define Reducer 2
	def reducer_sort_movies(self, key, values):
        for movie in values:
            yield movie, key

if __name__ == '__main__':
    MovieSorted.run()

