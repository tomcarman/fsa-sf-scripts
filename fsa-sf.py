import os
import json
import untangle
import requests
import csv
from tqdm import tqdm
import pymongo
from pymongo import MongoClient
from pymongo.errors import BulkWriteError


HEADERS = {'x-api-version': '2'}

REFRESH_FILES = False
INPUT_FILE = 'input/input.csv'
OUTPUT_FILE = 'input/output.csv'
FILE_DIRECTORY = 'files'



def run():

	if REFRESH_FILES:
	# In future we should check for only new files and then only fetch them. 
	# For now, we will delete and re-download all.

		print('Refreshing FSA data, this will take a while...')

		print('Dropping the existing database...')
		dropDatabase()

		print('Deleting existing files...')
		deleteExistingFiles()

		print('Getting a list of files for downloading...')
		file_urls = getFileUrls()

		print('Downloading files...')
		downloadFiles(file_urls)

		print('Parsing the files ready for database...')
		establishments = untangleFiles()

		print('Loading to mongodb, this might take a while...')
		loadToDb(establishments)

		print('Building the index, this might take a while...')
		buildIndex()


	processRestaurants()
	print('Finished')



# Refresh Methods
def dropDatabase():
	client = MongoClient('localhost', 27017)
	database = client['fsa_db']
	table = database['establishments']
	table.drop()


def deleteExistingFiles():
	for filename in os.listdir(FILE_DIRECTORY):
		if filename.endswith('.xml'):
			os.remove(os.path.join(FILE_DIRECTORY, filename))

def buildIndex():
	client = MongoClient('localhost', 27017)
	database = client['fsa_db']
	table = database['establishments']
	table.create_index([('name', pymongo.TEXT)])






def getFileUrls():

	authorities_endpoint = 'https://api.ratings.food.gov.uk/authorities'

	response_raw = requests.get(authorities_endpoint, headers = HEADERS)
	response = json.loads(response_raw.content)

	file_urls = {}

	for authority in response['authorities']:
		file_urls[authority['FriendlyName']] = authority['FileName']

	return file_urls


def downloadFiles(file_urls):

	for name, url in file_urls.items():

		print(f'Processing {name} - {url}')
		response = requests.get(url, headers = HEADERS, stream=True)
		
		with open('files/'+name+'.xml', "wb") as file:
			for data in tqdm(response.iter_content()):
				file.write(data)


def untangleFiles():

	establishments = []


	for filename in os.listdir(FILE_DIRECTORY):

		if filename.endswith('.xml'):

			print(f'Processing {filename}...')

			file = open(os.path.join(FILE_DIRECTORY, filename), 'r')

			obj = untangle.parse(file)

		
			try:

				for establishment_raw in obj.FHRSEstablishment.EstablishmentCollection.EstablishmentDetail:

					establishment = Establishment(establishment_raw)
					establishments.append(establishment.__dict__)
			
			except AttributeError:

				print(f'Failed to parse the file: {filename}')


	return establishments




def loadToDb(establishments):

	client = MongoClient('localhost', 27017)
	database = client['fsa_db']
	table = database['establishments']


	try:

		results = table.insert_many(establishments, ordered=False)


	except BulkWriteError as bwe:

		for error in bwe.details['writeErrors']:
			
			if error['code'] == 11000:
				record_id = error['keyValue']['_id']
				print(f'Already exists: \'{record_id}\'')



def processRestaurants():


	all_results = []


	with open (INPUT_FILE) as csv_file:

		csv_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
		next(csv_reader) # Skip headers


		client = MongoClient('localhost', 27017)
		database = client['fsa_db']
		table = database['establishments']

		row_number = 0

		for row in csv_reader:

			row_number = row_number+1

			name = row[1]
			postcode = row[5].upper().replace(' ', '')

			print(f'{row_number} - Processing {name} - {postcode}')

			query = {
					"$and":
						[
							{
							"postcode": postcode
							},
							{
							"$text":
								{
									"$search": name,
									"$caseSensitive": False,
									"$diacriticSensitive": False
								}
							}
						]
					}					

			results = []

			for doc in table.find( query ):
				results.append(doc)


			if len(results) == 0:
				row.append('No match')

			elif len(results) > 1:
				row.append('Multiple matches')

			elif len(results) == 1:
				row.append('Single match')
				row.append(doc['rating'])
				row.append(doc['ratingdate'])
				row.append(doc['_id'])

			all_results.append(row)

	with open(OUTPUT_FILE, 'w') as csv_outfile:

		print('Writing results to output.csv...')

		csv_writer = csv.writer(csv_outfile)
		csv_writer.writerow(['Id', 'Restaurant', 'Street 1', 'Street 2', 'City', 'Postcode', 'CreatedDate', 'Match', 'Hygiene Rating', 'Rating Date', 'FSA ID'])
		csv_writer.writerows(all_results)


class Establishment:

	def __init__ (self, obj):

		self.name = obj.BusinessName.cdata
		self._id = obj.FHRSID.cdata
		self.rating = obj.RatingValue.cdata
		self.ratingdate = obj.RatingDate.cdata

		if hasattr(obj, 'AddressLine1'):
			self.address1 = obj.AddressLine1.cdata
		if hasattr(obj, 'AddressLine2'):
			self.address2 = obj.AddressLine2.cdata
		if hasattr(obj, 'AddressLine3'):
			self.address3 = obj.AddressLine3.cdata
		if hasattr(obj, 'AddressLine4'):
			self.address4 = obj.AddressLine4.cdata
		if hasattr(obj, 'PostCode'):
			self.postcode = obj.PostCode.cdata.replace(' ', '')


run()	
