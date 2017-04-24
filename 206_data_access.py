###### INSTRUCTIONS ###### 

# An outline for preparing your final project assignment is in this file.

# Below, throughout this file, you should put comments that explain exactly what you should do for each step of your project. You should specify variable names and processes to use. For example, "Use dictionary accumulation with the list you just created to create a dictionary called tag_counts, where the keys represent tags on flickr photos and the values represent frequency of times those tags occur in the list."

# You can use second person ("You should...") or first person ("I will...") or whatever is comfortable for you, as long as you are clear about what should be done.

# Some parts of the code should already be filled in when you turn this in:
# - At least 1 function which gets and caches data from 1 of your data sources, and an invocation of each of those functions to show that they work 
# - Tests at the end of your file that accord with those instructions (will test that you completed those instructions correctly!)
# - Code that creates a database file and tables as your project plan explains, such that your program can be run over and over again without error and without duplicate rows in your tables.
# - At least enough code to load data into 1 of your dtabase tables (this should accord with your instructions/tests)

######### END INSTRUCTIONS #########
# Name: Michael Braunstein
# Option chosen: 2

# Put all import statements you need here.
import unittest
import itertools
import requests
import collections 
import tweepy
import twitter_info
import json
import sqlite3

# Begin filling in instructions....
# Authentication information should be in a twitter_info file...
consumer_key = twitter_info.consumer_key
consumer_secret = twitter_info.consumer_secret
access_token = twitter_info.access_token
access_token_secret = twitter_info.access_token_secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Setting up library to grab stuff from twitter with your authentication, and return it in a JSON format 
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())

CACHE_FNAME = "SI206_finalproject_cache.json"
# Setting up my chache file here that I will ater use to cache all my data 
try: 
	cache_file = open(CACHE_FNAME, 'r')
	cache_contents = cache_file.read()
	cache_file.close()
	CACHE_DICTION = json.loads(cache_contents)
except:
	CACHE_DICTION = {}

# Here I will create a function to pull data from OMDb API and caching it as well: 
# This function will take input of a movie title and then return a dictionary of data about that movie, and cache the data and the cache file. 
def get_movie_data(movie_str):
	unique_identifier = "OMDb_{}".format(movie_str)
	if unique_identifier in CACHE_DICTION:
		#print ("Using cached data for", movie_str)
		movie_dict = CACHE_DICTION[unique_identifier]
	else:
		#print ("getting data from internet for", movie_str)
		baseurl = 'http://www.omdbapi.com/?'
		url_params = {'t': movie_str, 'y': '2017'}
		response = requests.get(baseurl, params = url_params)
		movie_dict = response.json()
		CACHE_DICTION[unique_identifier] = movie_dict
		f = open(CACHE_FNAME, 'w')
		f.write(json.dumps(CACHE_DICTION))
		f.close()

	return movie_dict

# Here is where I will pull and cache my twitter search data about a movie	
# Within this function, I am able to search for a title of a movie and pull the tweets where people have mentioned the movie title. 
def get_twitter_data(search):
	unique_identifier = "twitter_{}".format(search)
	if unique_identifier in CACHE_DICTION:
		#print ("using cached data for", search)
		twitter_results = CACHE_DICTION[unique_identifier]
	else:
		#print ("getting data from internet for", search)
		twitter_results = api.search(q=search)
		CACHE_DICTION[unique_identifier] = twitter_results
		f = open(CACHE_FNAME, 'w')
		f.write(json.dumps(CACHE_DICTION))
		f.close()
	tweets = twitter_results['statuses']
	return tweets




# for tweet in beauty:
# 	print (tweet['text'])
# 	print (tweet['user']['favourites_count'])
# 	print (tweet['retweet_count'])
# 	print ()

# Here is where I will be creating my data tables: 
# First table will be for my movies data. 

conn = sqlite3.connect('final_project.db') # These two lines are to start off making the database
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS FINAL_PROJECT')

#Movies table
table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Movies (movie_id TEXT PRIMARY KEY, ' #The movie ID will be the primary key
table_spec += 'title TEXT, director TEXT, num_languages INTEGER, rating INTEGER, box_office TEXT, top_actor TEXT)'
cur.execute(table_spec)

#Tweets table
table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Tweets (tweet_id TEXT PRIMARY KEY, '
table_spec += 'tweet_text TEXT, user_id TEXT, associated_movie TEXT, num_retweets INTEGER, num_favorites INTEGER)'
cur.execute(table_spec)

#Users table
table_spec = 'CREATE TABLE IF NOT EXISTS '
table_spec += 'Users (user_id TEXT PRIMARY KEY, '
table_spec += 'screen_name TEXT, user_favorites INTEGER)'
cur.execute(table_spec)

# Here are my three insert statements I will call later in order to input my data into my database. 
statement = 'INSERT OR IGNORE INTO Movies VALUES (?, ?, ?, ?, ?, ?, ?)'
statement1 = 'INSERT OR IGNORE INTO Tweets VALUES (?, ?, ?, ?, ?, ?)'
statement2 = 'INSERT OR IGNORE INTO Users VALUES (?, ?, ?)'



# Here I will be making my first class that will work with Movie data. 
class Movie(): #This is my creation of the class
	def __init__(self,movie_dict): #Defining my constructor that will take in the movie_dict of data
		self.movie = movie_dict
		self.title = movie_dict['Title']
		self.year = movie_dict["Year"]
		self.movie_id = movie_dict['imdbID']
		self.box_office = movie_dict["BoxOffice"]
		self.director = movie_dict['Director']
		self.languages = len(movie_dict["Language"].split())
		self.rating = movie_dict["imdbRating"]

	def get_actors(self): # Here is my get_actors method that will return a list of all the actors in the movie
		actors = self.movie['Actors']
		return actors
	
	# This is where I will create my get_movie_table method which will return a tuple of information that can be directly put into the movie data table once this method is called on an instance of a movie. 
	def get_movie_table(self):
		actors = Movie.get_actors(self)
		lst = actors.split(", ")
		main_actor = lst[0]
		t = self.movie_id, self.title, self.director, self.languages, self.rating, self.box_office, main_actor
		return t

	
	def __str__(self): # Here is my string method that returns a string about the movie. This will focus on certain aspects fo the movie that I think are important, such as the title, who it was directed by, notable actors, and rating.  
		return self.title + " was directed by " + self.director  +" with notable actors being " + self.get_actors() + ". This movie had an IMDM rating of " + str(self.rating) + " and a box office performance of " + self.box_office + ". It was created in " + str(self.languages) + " language(s) and came out in " + str(self.year) + "."

# This is where I create my list of movies I will find data on, stored in variable movies
movies = ["Beauty and the Beast", "The Boss Baby", "Logan"]

# This is where I will pull data about these movies and store the data in movies_dicts variable, where I will be using the get_movie_data on each iteration of the list. 
movies_dicts = []
for movie in movies: 
	movies_dicts.append(get_movie_data(movie))

#This is where I will be creating instances of each movie and storing each of those instances in a list varaible named movie_insts
movie_insts = []
for movie in movies_dicts:
	movie_insts.append(Movie(movie))

# This is where I will be inputting the data about each movie into the Movies data table. 
for m in movie_insts:
	cur.execute(statement, m.get_movie_table())

# Here is where I will be creating my Twitter_search class which will take in a tweet dictionary as input in order to craete a instance. 
# All of the instance variables will be pieces that I will then later use to input into my Tweets data table
class Twitter_search():
	def __init__(self, tweet, movie):
		self.tweet = tweet
		self.text = tweet['text']
		self.id = tweet['id_str']
		self.userid = tweet['user']['id_str']
		self.retweets = tweet['retweet_count']
		self.associated_movie = movie
		self.screen_name = tweet['user']['screen_name']
		self.favorites = tweet['favorite_count']
		self.user_favorites = tweet["user"]["favourites_count"]

	# This method, get_twitter_table will return the tuple of information about each tweet that I can then load into the data table. I figured that I can reutrn the data and then inout it right into the table later on in the code. 
	def get_twitter_table(self):
		t1 = self.id, self.text, self.userid, self.associated_movie, self.retweets, self.favorites
		return t1
	def get_users_table(self):
		t2 = self.userid, self.screen_name, self.user_favorites
		return t2

# This is where I am defining my hashtags that I will collect data on, hashtags to be stored in variable hashtags
hashtags = ['#beautyandthebeast', '#thebossbaby', '#Logan']

#This is where I will pull data from twitter and store that data about each hashtag and tweets associated with it in variable hashtag_tweets
hashtag_tweets = []
for tag in hashtags:
	hashtag_tweets.append(get_twitter_data(tag))

# This is where I will be making instances of my Twitter_search class that will each bring in data about the tweets relevant to a movie as well as the movie title itself. All of this instances should be stored in a list named twitter_insts.
twitter_insts = []
for i in range(len(hashtag_tweets)):
	for tweet in hashtag_tweets[i]:
		twitter_insts.append(Twitter_search(tweet, movies[i]))

# This is where I will be loading data about each movies tweets into the Tweets table. 
for inst in twitter_insts:
	cur.execute(statement1, inst.get_twitter_table())
	cur.execute(statement2, inst.get_users_table())


# This is where I will pull data about users for the User table. 
# This data will be pulled from tweets, specifically the users mentioned. This data will also be stored in the cache file. 
for tweets in hashtag_tweets:
	for tweet in tweets:
		for user in tweet['entities']['user_mentions']:
			unique_identifier = "user_{}".format(user['screen_name'])
			if unique_identifier in CACHE_DICTION:
				my_var = CACHE_DICTION[unique_identifier]
			else:
				my_var = api.get_user(user['screen_name'])
				CACHE_DICTION[unique_identifier] = my_var
				f = open(CACHE_FNAME, 'w')
				f.write(json.dumps(CACHE_DICTION))
				f.close()
			t2 = (my_var['id_str'],my_var['screen_name'],my_var['favourites_count'])
			cur.execute(statement2,t2)


# Committing all of the statements to the data tables
conn.commit()


file_summary = open("final_project_summary.txt", 'w')
file_summary.write("Michael Braunstein\n")
file_summary.write("SI 206 Final Project Output Summary Page\n\n")
file_summary.write("For this project, I performed a lot of data analytics on current movies and how much people were talking about each respective movie on Twitter. This summary page should give you a better idea of what movie you should go see based on these statistics. \n\nThe movies I decided to look at were Beauty and the Beast, The Boss Baby, and Logan. ")

# Query statements:


# Pull all of the data from the Tweets table where number of retweets is greater than 0, and save that data in list with variable name being tweets_for_rts
statement = 'SELECT * FROM Tweets WHERE num_retweets > 20'
result = cur.execute(statement)
tweets_for_rts = []
for r in result.fetchall():
	tweets_for_rts.append(r)

# Sort this list by the number of retweets and return a list of the 3 most retweeted tweets, save in in a variable named most_popular_tweets
sorted_tweets_for_rts = sorted(tweets_for_rts, key = lambda x: x[4], reverse = True)
most_popular_tweets = sorted_tweets_for_rts[:3]
most_popular_tweets_str = "After searching for tweets about each movie, here were the three most popular tweets and a little information about each of them. Note: Popular tweets means they had the most retweets\n" 
file_summary.write(most_popular_tweets_str)
for i in range(len(most_popular_tweets)):
	file_summary.write("\n")
	file_summary.write(str(i+1) + ". Tweet Text: " +str(most_popular_tweets[i][1]))
	file_summary.write( "\nAssociated Movie: " + str(most_popular_tweets[i][3]))
	file_summary.write( "\nTweet Id: "+ str(most_popular_tweets[i][0]))
	file_summary.write( "\nUser Id: "+ str(most_popular_tweets[i][2]))
	file_summary.write( "\nNumber of Retweets: " + str(most_popular_tweets[i][4]))
	file_summary.write("\n")





#print (tweets_for_rts)


# Make an inner join query statement to find the screen name, associated movie, user favorites and their number of retweets on their tweet from the inner join of Tweets and Users tables where the number of rewtweets and user favorites are both greater than 50, then save that resulting list of tuples in a dictionary named variable most_popular_movies_tweeters
# Make sure you do this using dictonary comprehension
statement = 'SELECT screen_name, associated_movie, user_favorites, num_retweets FROM Tweets INNER JOIN Users ON Tweets.user_id=Users.user_id WHERE num_retweets >50 AND user_favorites > 50'
result = cur.execute(statement)
#most_popular_movies_tweeters = {r[1]:(r[0],r[2],r[3]) for r in result.fetchall()}
most_popular_movies_tweeters = {r[0]:(r[1],r[2],r[3]) for r in result.fetchall()}
# sorted_most_popular_movies_tweeters = sorted(most_popular_movies_tweeters, key = lambda x: most_popular_movies_tweeters[x][0])
# print (sorted_most_popular_movies_tweeters)
# for r in result.fetchall():
# 	most_popular_movies_tweeters.append(r)
print (most_popular_movies_tweeters)
print ('**************')

tweeters_str = "\nWhen looking at users who tweeted about certain movies, I looked at what popular users (people who had over 50 user favorites and 50 retweets on their tweet) and what movie they tweeted about. It becomes evident that certain movies attract more attention than others:\n"
file_summary.write(tweeters_str)
#file_summary.write("The statistics after the username are as follows: (Movie tweeted about, number of user favorites, number of retweets)\n")
for key in most_popular_movies_tweeters.keys():
	file_summary.write("\n")
	file_summary.write("User: " + key + "\n Associated Movie: " +str(most_popular_movies_tweeters[key][0]) + "\n User Favorites: " + str(most_popular_movies_tweeters[key][1]) + "\n Number of Tweet Retweets: " + str(most_popular_movies_tweeters[key][2]))
	file_summary.write("\n")

# Use dictionary accumulation in order to calculate the total number of retweets each of the associated movies have had. 
# Each key should be the associated movie and each value should add all of the retweets together. 
movie_retweets = {}
for tweet in tweets_for_rts:
	if tweet[3] not in movie_retweets:
		movie_retweets[tweet[3]] = tweet[4]
	else:
		movie_retweets[tweet[3]] += tweet[4]
file_summary.write("\n\nWhen looking at each movie, here are the total number of retweets on the tweets pulled associated to each movie:\n")
for movie in movie_retweets.keys():
	file_summary.write(movie + " Retweets:  " + str(movie_retweets[movie]))
	file_summary.write("\n")
# print (movie_retweets)

# Then sort a list of the dictionary keys based on the number of total number of retweets for each movie, and store the top movie and its number of retweets in a varaiable called top_movie_retweets.
sorted_movie_retweets = sorted(movie_retweets, key = lambda x: movie_retweets[x], reverse = True)
#print (sorted_movie_retweets)
top_movie_retweets = [sorted_movie_retweets[0], movie_retweets[sorted_movie_retweets[0]]]
file_summary.write("\nThe most tweeted about movie: " +str(top_movie_retweets[0]) + " with " +str(top_movie_retweets[1]) + " retweets!\n")
print (top_movie_retweets)


# Pull the movie title, rating, and box office performance for each movie with a query statment. Save this information in a list named movie_performances.
# Save this information by using list comprehension
statement = 'SELECT title, rating, box_office FROM Movies'
result = cur.execute(statement)
movie_performances = [r for r in result.fetchall()]
sorted_movie_performances = sorted(movie_performances, key = lambda x: x[1], reverse = True)
file_summary.write("\nHere were the performances of each of the three movies (listed by their ratings), found from the IMDb database:\n\n")
for movie in sorted_movie_performances:
	file_summary.write(movie[0] + ": Rating: " + str(movie[1]) + " Box Office Performance: " + str(movie[2]))
	file_summary.write("\n")
#print (movie_performances)


# Use a query to pull the screen_name and user favorites from the Users table for users that have over 50 favorites and save the resulting tuples in a list named top_users 



# Use dictionary comprehension to merge two dictionaries where the keys will be the movie title and the values will be a list of number of retweets, rating, and box office performance, and save this data in a dictionary titled movie_dict_stats

# Use a query to find all the lead actors in the movies you are looking at. Then use set comprehension in order to store those actors without the possibility of one repeating in a varaible named movie_actors 
statement = 'SELECT top_actor FROM Movies'
result = cur.execute(statement)
movie_actors = {r[0] for r in result.fetchall()}

file_summary.write("\nFrom these movies, the main actors were:\n")
for actor in movie_actors:
	file_summary.write(actor)
	file_summary.write("\n")
print (movie_actors)


file_summary.write("\n\nFrom all this data, the movie I would recommend you see is: " + top_movie_retweets[0] + "\n")
if top_movie_retweets[0] == "Beauty and the Beast":
	file_summary.write(movie_insts[0].__str__())
elif top_movie_retweets[0] == "The Boss Baby":
	file_summary.write(movie_insts[1].__str__())
elif top_movie_retweets[0] == "Logan":
	file_summary.write(movie_insts[2].__str__())

file_summary.write("\n\n\n\n")

file_summary.close()
# Write file named final_project_summary that will have all your findings in it. 



#Write more test cases





### IMPORTANT: MAKE SURE TO CLOSE YOUR DATABASE CONNECTION AT THE END OF THE FILE HERE SO YOU DO NOT LOCK YOUR DATABASE (it's fixable, but it's a pain). ###
conn.close()



# Put your tests here, with any edits you now need from when you turned them in with your project plan.
# Write your test cases here.
print ("\n\n BELOW THIS LINE IS OUTPUT FROM TESTS:\n")

class Task1(unittest.TestCase):
	def test_tweet_caching(self):
		name ="SI206_finalproject_cache.json"
		f = open(name, 'r')
		self.assertTrue("Beauty and the Beast" in f.read())
		f.close()
	def test_movie_caching(self):
		name ="SI206_finalproject_cache.json"
		f = open(name, 'r')
		self.assertTrue("OMDb_Logan" in f.read())
		f.close()
	def test_get_user_tweets(self):
		res = get_twitter_data("#Logan")
		self.assertEqual(type(res), type(['hi', 5]))
	def test_movie_title(self):
		mov = get_movie_data("Logan")
		i = Movie(mov)
		self.assertEqual(i.title,"Logan")
	def test_get_actors(self):
		mov = get_movie_data("Logan")
		i = Movie(mov)
		self.assertEqual(type(i.get_actors()), type('hello'))
	def test_movies3(self):
		self.assertEqual(len(movies), 3)
	def test_get_user2(self):
		l = get_twitter_data("#beautyandthebeast")
		self.assertEqual(type(l[1]), type({}))
	def test_str(self):
		mov = get_movie_data("Logan")
		i = Movie(mov)
		self.assertIn(str(i.title), i.__str__())
	def test_tweet_table_method(self):
		self.assertIn(twitter_insts[0].text, twitter_insts[0].get_twitter_table())
	def test_tweet_class(self):
		self.assertIn(twitter_insts[0].associated_movie, ['Beauty and the Beast', "Logan", 'The Boss Baby'])
	def test_user_table_method(self):
		self.assertIn(twitter_insts[0].userid, twitter_insts[0].get_users_table())
	def test_get_movie_table_method(self):
		mov = get_movie_data("Beauty and the Beast")
		i = Movie(mov)
		self.assertIn("Bill Condon", i.get_movie_table())






## Remember to invoke all your tests...

if __name__ == "__main__":
	unittest.main(verbosity=2)


