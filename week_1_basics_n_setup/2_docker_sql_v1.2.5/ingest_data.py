from time import time
from sqlalchemy import create_engine
import pandas as pd
import argparse
import os
import urllib.request
import gzip

def main(params):
	user = params.user
	password = params.password
	host = params.host
	port = params.port
	db = params.db
	table_name = params.table_name
	# if there is not params.url then url = ""
	url = params.url
	if url == "":
		url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-01.csv.gz"

	csv_name = 'output.csv'

	# download the csv
	# os system function can run command line arguments from Python
	file_name_gz = "output.csv.gz"

	urllib.request.urlretrieve(url, file_name_gz)

	with gzip.open(file_name_gz, 'rb') as f_in:
		with open(csv_name, 'wb') as f_out:
			f_out.write(f_in.read())
	print('Downloaded the CSV')

	engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

	df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
	df = next(df_iter)

	df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
	df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

	#  adding the column names
	df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

	# adding the first batch of rows
	df.to_sql(name=table_name, con=engine, if_exists="append")

	while True:
		t_start = time()

		df = next(df_iter)

		df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
		df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

		df.to_sql(name=table_name, con=engine, if_exists="append")

		t_end = time()

		print('Inserted another chunk... took %.3f second(s)' % (t_end - t_start))

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")

	# user
	# password
	# host
	# port
	# database name
	# table name
	# url of the csv

	parser.add_argument('--user', help="user name for postgres")
	parser.add_argument('--password', help="password for postgres")
	parser.add_argument('--host', help="host for postgres")
	parser.add_argument('--port', help="port for postgres")
	parser.add_argument('--db', help="database name for postgres")
	parser.add_argument('--table_name', help="name of the table where we will write the results to")
	parser.add_argument('--url', help="url of the CSV")

	args = parser.parse_args()

	# xprint(args.accumulate(args.integers))

	main(args)
