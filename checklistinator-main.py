import streamlit as st
import pandas as pd
import pyarrow
import polars as pl
import os
import numpy as np
import re
import sys
import datetime
import json
from datetime import datetime
import time as tt
import csv
import itertools
from itertools import combinations
import google.cloud
import google.oauth2
from google.oauth2 import service_account
import gcsfs
import psutil
import tracemalloc
import gc

#tracemalloc.start()
#Getting secrets in order to access my google cloud account (we don't want to expose the raw files to anyone)
#Converting the secrets to a dict
service_account_json_str = st.secrets["gcs"]["service_account"]
credentials_dict = json.loads(service_account_json_str)
credentials_json = json.dumps(credentials_dict)

# Test access
#st.write(credentials_dict["project_id"])

#st.write(credentials_dict)

#being able to get the files
fs = gcsfs.GCSFileSystem(token=credentials_dict)
#st.write(fs)
#st.write(fs.ls("birds-data/checklistinator"))
st.set_page_config(layout="wide") 

#Default number of rows for polars DataFrames is now 55
pl.Config(tbl_rows=55)

def report_memory():
	process = psutil.Process(os.getpid())
	mem = process.memory_info().rss / 1024 ** 2  # Convert bytes to MB
	st.write(f"💾 Current memory usage: {mem:.2f} MB")

#Defining text that I want to come out in a strean
COLLA = """
Collating...
"""


def stream_data_c():
    for word in list(COLLA):
        yield word + " "
        tt.sleep(0.15)
        
COLLE = """
Collecting...
"""


def stream_data_co():
    for word in list(COLLE):
        yield word + " "
        tt.sleep(0.125)

CALC = """
Calculating...
"""


def stream_data_ca():
    for word in list(CALC):
        yield word + " "
        tt.sleep(0.1)
        
TFC = """
\n The following combinations will be used:
"""


def stream_data_combos():
    for word in TFC.split(" "):
        yield word + " "
        tt.sleep(0.04)
        
NUMBER = """
\n A list of locations by number of co-occurrences:
"""


def stream_data():
    for word in NUMBER.split(" "):
        yield word + " "
        tt.sleep(0.02)

        
COOC = """
\n A list of locations by percentage:
"""


def stream_data_1():
    for word in COOC.split(" "):
        yield word + " "
        tt.sleep(0.02)


url = "https://ebird.org/data/download"

NOTE = f"""
\n Note that this includes the top 10 results from the raw occurance counts to the left.
"""


def stream_data_note():
    for word in NOTE.split(" "):
        yield word + " "
        tt.sleep(0.02)

CHECK = f"""
\n Data summaries are derived from eBird Basic Dataset. Raw data are not distributed by this app. For full checklist access, please visit {url}!
"""


def stream_data_2():
    for word in CHECK.split(" "):
        yield word + " "
        tt.sleep(0.02)


CITATION = f"""
CITATION: eBird Basic Dataset. Version: EBD_relJun-2025. Cornell Lab of Ornithology, Ithaca, New York. Jun 2025.
"""


def stream_data_cit():
    for word in CITATION.split(" "):
        yield word + " "
        tt.sleep(0.02)
        

#Loading the dictiory with all place names and their codes 'Prince George's, Maryland, United States':'US-MD-033'
with open("data/big_dict.json", "r") as file:
    big_dict_loaded = json.load(file)

possible_places = list(big_dict_loaded.keys())

#Reading a csv for possible species to enter
possible_species = pl.read_csv('data/Species.csv', separator=',')

area_lists = []

#setting up cosmetics
st.markdown(
    """
    <style>
    /* Style the multiselect box */
    div[data-baseweb="input"] > div {
        background-color: #f6fff6;  /* Very light yellow */
        color: #003300;  /* Dark green text */
    }

	div[data-baseweb="input"] > div,
	div[data-baseweb="input"] input,
	div[data-baseweb="input"] span {
    	font-family: Verdana, sans-serif;
	}
	
	li[data-baseweb="input"] {
    color: #2e8b57;  /* SeaGreen */
    font-weight: bold;
    font-family: Verdana, sans-serif;
    font-size: 1.1em;
	}
	
	label[data-testid="stWidgetLabel"] {
    color: #2e8b57;  /* SeaGreen */
    font-weight: bold;
    font-family: Verdana, sans-serif;
    font-size: 1.1em;
	}
	
    /* Optional: Change background or text color */
    div[data-baseweb="select"] > div {
        background-color: #f6fff6;  /* Very light green */
        color: #003300;  /* Dark green text */
    }
    
    div[data-baseweb="select"] > div,
	div[data-baseweb="select"] input,
	div[data-baseweb="select"] span {
    	font-family: Verdana, sans-serif;
	}
	
	div[data-baseweb="base-input"] > div,
	div[data-baseweb="base-input"] input,
	div[data-baseweb="base-input"] span {
    	font-family: Verdana, sans-serif;
	}
    </style>
    """,
    unsafe_allow_html=True
)

#trying to delete variables to save space
if "df" in globals():
    del df
    gc.collect()

#No native way to center, but here's where writing starts
col1, col2 = st.columns([2.25,10])
with col2:
	st.title("Welcome to the Checklistinator!!!")
	st.write("Updated to June 2025")
	st.write("**For best results: Search using proper nouns (Oman, not oman; Snow Goose, not snow goose)**")

species = st.multiselect(
    "Please Enter Species",
   (possible_species),
   default=None,
   placeholder="Select a species from this menu"
)

if len(species) > 2:
	sharpness = st.slider("How many species to match?", 1, len(species), len(species))
else:
	sharpness = len(species)
#	st.write("I want", sharpness, "species matched")

#species.append(species)

#st.write(species)

place_inputted_user = st.selectbox(
	"Please Enter Locality",
	options = (possible_places),
	index=None,
   placeholder="Select a place from this menu"
)

min_check = st.text_input("(Optional) Input a minimum number of occurances needed for percentage reporting", "10" )

st.write('If you just want all occurances ever, you can just search from 01-01 - 12-31')
start_date = st.text_input("Please input a start date (YYYY-MM-DD) or (MM-DD):", placeholder="Examples: 2017-07-27; 01-01; 09-10")
end_date = st.text_input("Please input an end date (YYYY-MM-DD) or (MM-DD):", placeholder="Examples: 2025-06-30; 12-31; 03-05" )



st.write('Press enter/return once you\'ve filled out all fields!')

#Make sure it doesn't go too soon
if not species:
	st.stop()
		
if not place_inputted_user:
	st.stop()

if not start_date:
	st.stop()

if not end_date:
	st.stop()
try:
	int(min_check)
except:
	st.write('Please enter a valid minimum checklist count')
	st.stop()
	


#f = str(place_inputted_user).split('-')
#

#What happens if there's a big area? California, for instance, is spread among like 50 county files. Here is how we deal with that
def big_area(area_list):
	st.write("You selected a large area! This might take some time...")
	all_result_placeval = []
	all_place_counts = []
	for checklist in area_list:
		#st.write(checklist)
		checklist_split = checklist.split("/")
		checklist_name = f"{checklist_split[2]}/{checklist_split[3]}"
		st.write(f"Collecting: {checklist_name}")
		combonotions = pl.DataFrame()
		dictionary = {}
		common_ids = set()
		common_ids_original = set()
		gcs_path = os.path.join("birds-data", checklist_name)
		#st.write(gcs_path)
		with fs.open(gcs_path, 'rb') as f:
			df = pl.scan_parquet(f)
		#Yeah I know that it should be called a combination, not a permutation
		if sharpness != len(species):
#			st.write("The following combinations will be used:")
			
			st.write_stream(stream_data_combos())
			
			nom = 1
			
			permutations = []
			
			diff = len(species) - sharpness
			for i in range(diff+1):
				for species_i in combinations(species, sharpness + i):
					permutations.append(species_i)
#			st.write(permutations)
			
			for sp in species: 
				filtered_1 = df.filter(pl.col("Common_Name") == sp).select(["Checklist_ID"]).collect()
				filtered_1_list = filtered_1["Checklist_ID"].to_list()
#				st.write(type(filtered_1))
				
#				columns_to_select = ["Checklist_ID"]
	#			available_columns = [col for col in columns_to_select if col in filtered_1.columns]
				#filtered_1 = filtered_1.select(available_columns)
#				st.write(filtered1)
				dictionary[sp] = filtered_1_list
	#			st.write(filtered_1_list)
				
				del(filtered_1)
				del(filtered_1_list)
				gc.collect()
				
			for combo in permutations:
				n = 1
				#st.write(combo)
				combodf = pl.DataFrame({"Combinations": [combo]})
#				st.write(type(combodf))
				combonotions = pl.concat([combonotions, combodf], how="vertical")
				for specc in combo:
#					st.write(specc)
					filtered_specc = dictionary[specc]
		#			st.write(filtered_specc)
					filtered_set = set(filtered_specc)
					if n == 1:
						common_ids_original = filtered_set
					else:
						common_ids_original = common_ids_original & filtered_set
					n = n + 1
#					st.write(common_ids_original)
#					st.write(type(common_ids_original))
				if nom == 1:
					common_ids = common_ids_original
				else:
					common_ids = common_ids | common_ids_original
				#st.write(common_ids)
				nom = nom + 1
			filtered = []
			for sp in species:
				query = (df.filter(pl.col("Common_Name").is_in(species)).collect())
				columns_to_select = ["Place", "Checklist_ID", "Observation_Date", "State", "County"]
				available_columns = [col for col in columns_to_select if col in query.columns]
				query = query.select(available_columns)
				filtered.append(query)
				del(query)
				gc.collect()
			if nom == len(area_list):
				st.write(combonotions)
		#this is what happens if it is the same, e.g., 6 out of 6 species
		else: 
			filtered = []
			for sp in species:
	#			st.write('starting')
				
				query = (df.filter(pl.col("Common_Name") == sp).collect())
				
				columns_to_select = ["Place", "Checklist_ID", "Observation_Date", "State", "County"]
				
				available_columns = [col for col in columns_to_select if col in query.columns]
				query = query.select(available_columns)
				
				filtered.append(query)
				del(query)
				gc.collect()
			#st.write(filtered)
			#st.write(f["Checklist_ID"])
			ids = [set(f["Checklist_ID"].to_list()) for f in filtered]
			

			common_ids = set.intersection(*ids)
	
		
		result = filtered[0].filter(pl.col("Checklist_ID").is_in(common_ids)).unique()
		del(filtered)
		del(common_ids)
		gc.collect()
		
		result = filter_by_date_range(df = result, start_date_str = str(start_date), end_date_str = str(end_date))
		result = result.select("Place", "Checklist_ID", "Observation_Date")
		result_try = result.group_by(["Place", "Observation_Date"]).len()
		#st.write(result_try)
		tryrty = result_try["Place"].value_counts()
		tryrty = tryrty.sort("count", descending=True)
		bad_places_df = tryrty.filter(pl.col("count") == 1)
		#st.write(bad_places)
		bad_places = bad_places_df['Place'].to_list()
#		st.write(bad_places)
#		st.write(tryrty)
		result = result.filter(~pl.col("Place").is_in(bad_places))
		lazy_df = df.unique(subset=["Checklist_ID"])
		checklist_placeval = lazy_df.select("Place", "Observation_Date").collect()
		checklist_placeval = filter_by_date_range(df = checklist_placeval, start_date_str = str(start_date), end_date_str = str(end_date))
		place_counts = checklist_placeval["Place"].value_counts()
		result_placeval = result["Place"].value_counts()
		all_result_placeval.append(result_placeval)
		all_place_counts.append(place_counts)
		del(place_counts)
		del(checklist_placeval)
		del(result_placeval)
		gc.collect()
#	st.write_stream(stream_data_ca())
	total_place_counts = pl.concat(all_place_counts).group_by("Place").sum()
#	st.write(total_place_counts)
	total_result_placeval = pl.concat(all_result_placeval).group_by("Place").sum()
#	st.write(total_result_placeval)
	placeval_df = total_place_counts.join(total_result_placeval, on="Place", how="left").fill_null(0)
	placeval_df = placeval_df.with_columns((
		(pl.col("count_right") / pl.col("count") * 100).round(2).alias("Co-occurrence Rate")
	))
	placeval_df = placeval_df.filter(pl.col("count_right") > 0)
#	st.write(placeval_df)
	top_results = total_result_placeval.sort("count", descending=True).head(10)
#	st.write(top_results)
	del(all_place_counts)
	del(all_result_placeval)
	gc.collect()
	top_results_percents = total_place_counts.join(top_results, on="Place", how="right").fill_null(0)
	del(total_place_counts)
	gc.collect()
	top_results_percents = top_results_percents.with_columns((
		(pl.col("count_right") / pl.col("count") * 100).round(2).alias("Co-occurrence Rate")
	))
	#st.write(top_results_percents)
	top_results_percents = top_results_percents.select(["Place", "Co-occurrence Rate", "count_right"])
#	st.write(top_results_percents)
	top_cocurrance = placeval_df.sort("Co-occurrence Rate", descending=True).select(["Place", "Co-occurrence Rate", "count_right"])
	top_cocurrance = top_cocurrance.filter(pl.col("count_right") >= int(min_check))
	top_cocurrance = top_cocurrance.head(20)
#	st.write(top_cocurrance)
	#st.write(top_cocurrance)
	final_df = top_results_percents.vstack(top_cocurrance).unique(subset=["Place"])
	total_result_placeval = total_result_placeval.rename({"count": "Count"})
	col1, col2 = st.columns([5,7])
	# st.write top 15 raw co-occurrence counts
	with col1:
		st.write_stream(stream_data())
		st.write(total_result_placeval.sort("Count", descending=True).head(30))
	final_df = final_df.rename({"count_right": "Count"})
	final_df = final_df.filter(pl.col("Count") >= int(min_check))
	with col2:
		st.write_stream(stream_data_1())
		st.write(final_df.sort("Co-occurrence Rate", descending=True).select(["Place", "Co-occurrence Rate", "Count"]))
		st.caption("\n Anything with less than " + str(min_check) + " reports at a location are not included.")
		st.caption("\n This includes the top 10 results from the raw occurance counts to the left (if they have enough reports).")
	st.write_stream(stream_data_2())
	st.write_stream(stream_data_cit())
	if "df" in st.session_state:
    		del st.session_state["df"]
	st.cache_data.clear()
	sys.exit()

	#total_place_counts = pl.concat(all_place_counts).group_by("Place").sum()
	#total_result_placeval = pl.concat(all_result_placeval).group_by("Place").sum()
	#placeval_df = total_place_counts.join(total_result_placeval, on="Place", how="left").fill_null(0)
	#placeval_df = placeval_df.with_columns([(pl.col("count_right") / pl.col("count") * 100).round(2).alias("co_occurrence_rate")])
	#st.write("\n A list of locations by number of co-occurrences:")
	#st.write(placeval_df.sort("count_right", descending=True).select(["Place", "count_right"]).head(15))
	#placeval_df = placeval_df.filter(pl.col("count_right") > 0)
	#st.write("\n A list of locations by percentage:")
	#st.write(placeval_df.sort("co_occurrence_rate", descending=True).select(["Place", "co_occurrence_rate"]).head(30))
	#st.write("\n A list of checklists:")
	#st.write(result.sort("Place", descending=True).head(20))
	#sys.exit()

def filter_by_date_range(df: pl.DataFrame, start_date_str: str, end_date_str: str, date_col = 'Observation_Date') -> pl.DataFrame:
	has_year = "-" in start_date_str and len(start_date_str.split("-")[0]) == 4
#	st.write(start_date_str.split("-")[0])
#	st.write(start_date_str.split("-"))
	df = df.with_columns([pl.col(date_col).str.strptime(pl.Date, "%Y-%m-%d").alias(date_col)])
#	st.write("Converted to dates")
	if has_year:
#		st.write("Years!")
		try:
			start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
			end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
			return df.filter((pl.col(date_col) >= start_date) & (pl.col(date_col) <= end_date))
		except:
			st.write("Please enter a valid date")
			st.stop()
	else:
		try:
			start_month = int(start_date_str.split("-")[0])
			start_day = int(start_date_str.split("-")[1])
			end_month = int(end_date_str.split("-")[0])
			end_day = int(end_date_str.split("-")[1])
		except:
			st.write("Please enter a valid date")
			st.stop()
#		st.write('splitting...')
		df = df.with_columns([pl.col(date_col).dt.month().alias("month"), pl.col(date_col).dt.day().alias("day")])
#		What if the dates wrap around the year?
		if (start_month, start_day) <= (end_month, end_day):
			return df.filter(
				((pl.col("month") > start_month) | ((pl.col("month") == start_month) & (pl.col("day") >= start_day))) &
				((pl.col("month") < end_month) | ((pl.col("month") == end_month) & (pl.col("day") <= end_day)))
			)
		else:
			return df.filter(
				((pl.col("month") > start_month) | ((pl.col("month") == start_month) & (pl.col("day") >= start_day))) |
				((pl.col("month") < end_month) | ((pl.col("month") == end_month) & (pl.col("day") <= end_day)))
			)
posible_files = []
def get_place(place_str: str):
	countries_w_states = ["US", "CA", "IN", "AU", "GB", "ES", "TW", "CO", "BR", "MX", "CR", "AR", "PE", "PT", "CL", "DE", "EC", "NZ", "PA"]
	states_w_counties = ["US-CA", "US-AZ", "US-FL", "US-CO", "US-WI", "US-IL", "US-MD", "US-NC", "US-OR", "US-MA", "US-MI", "US-NJ", "US-OH", "US-PA", "US-TX", "US-VA", "US-WA", "CA-QC", "CA-BC", "CA-ON", "GB-ENG", "US-NY"]
	global input_file
	global level
	global place_original
	possible_file_paths = [f[:-8] for f in fs.ls("birds-data/checklistinator")]
	for f in possible_file_paths: 
		#st.write(f)
		posible_file = f.split('/')[2]
		posible_files.append(posible_file)
	#st.write(posible_files)
	place_original = place_str
	place = place_original
	if place in posible_files:
		input_file = f"{place}.parquet"
#		st.write(input_file)
	elif len(place.split('-')) ==1:
		pattern = re.compile(f"^{re.escape(place_original)}")
		st.write(pattern)
		matching_files = [f[:-8] for f in fs.ls("birds-data/checklistinator") if pattern.match(f)]
		st.write(len(matching_files))
		for f in matching_files:
			#st.write(f)
			path = os.path.join('checklistinator', f)
			area_lists.append(path)
#			st.write("appended!")
		big_area(area_lists)
	else:
		placesplit = place.split('-')
	#	st.write(placesplit)
		#st.write(place_original)
		place = f"{placesplit[0]}-{placesplit[1]}"
		if place in posible_files:
			input_file = f"{place}.parquet"
#			st.write(input_file)
		else:
			placesplit = place.split('-')
			place = placesplit[0]
			if place in posible_files:
				st.write("possible!")
				input_file = f"{place}.parquet"
#				st.write(input_file)
			else:
				st.write("else!")
				if str(place_original) in countries_w_states:
					st.write("it's in states!")
					pattern = re.compile(f"^{re.escape(place_original)}")
					matching_files = [f for f in fs.ls("birds-data/checklistinator") if pattern.match(f)]
					st.write(len(matching_files))
					for f in matching_files:
						area_lists.append(f)
						st.write("appended!")
					big_area(area_lists)
				if str(place_original) in states_w_counties:
					st.write("it's in counties!")
					pattern = re.compile(f"^{re.escape(place_original)}")
					st.write(pattern)
					matching_files = [f for f in fs.ls("birds-data/checklistinator") if pattern.match(os.path.basename(f))]
					st.write(len(matching_files))
					for f in matching_files:
						path = os.path.join('checklistinator', f)
						area_lists.append(path)
#						st.write("appended!")
					big_area(area_lists)
				st.write("Not a possible locality")

#Loading the place we want
real_place = big_dict_loaded.get(place_inputted_user)

get_place(place_str = str(real_place))
filename = os.path.join('checklistinator', input_file)

if not filename:
	st.stop()


st.write_stream(stream_data_co())

#fs.ls("birds-data/data")

#st.write(filename)

gcs_path = os.path.join("birds-data", *filename.split("/"))
#st.write(gcs_path)


#Opening our db
with fs.open(gcs_path, 'rb') as f:
#	data = f.read(1024)
#	st.write(f"First 1 KB: {len(data)} bytes read")
	df = pl.scan_parquet(f)
#st.write("loaded")



#st.write(df.collect().head())

dictionary = {}
common_ids = set()
common_ids_original = set()

st.write_stream(stream_data_c())



#st.write("Co-occurance of all selected:")
#st.write(common_ids)

combonotions = pl.DataFrame()

#Here's what happens if they want to, like, match 3 out of 6 species (note that it includes 4/6, 5/6, and 6/6)
#Yeah I know that it should be called a combination, not a permutation
if sharpness != len(species):
#	st.write("The following combinations will be used:")
	
	st.write_stream(stream_data_combos())
	
	nom = 1
	
	permutations = []
	
	diff = len(species) - sharpness
	for i in range(diff+1):
		for species_i in combinations(species, sharpness + i):
			permutations.append(species_i)
#	st.write(permutations)
	
	for sp in species: 
		filtered_1 = df.filter(pl.col("Common_Name") == sp).select(["Checklist_ID"]).collect()
		filtered_1_list = filtered_1["Checklist_ID"].to_list()
#		st.write(type(filtered_1))
		
#		columns_to_select = ["Checklist_ID"]
	#	available_columns = [col for col in columns_to_select if col in filtered_1.columns]
		#filtered_1 = filtered_1.select(available_columns)
#		st.write(filtered1)
		dictionary[sp] = filtered_1_list
	#	st.write(filtered_1_list)
		
		del(filtered_1)
		del(filtered_1_list)
		gc.collect()
		
	for combo in permutations:
		n = 1
		#st.write(combo)
		combodf = pl.DataFrame({"Combinations": [combo]})
#		st.write(type(combodf))
		combonotions = pl.concat([combonotions, combodf], how="vertical")
		for specc in combo:
#			st.write(specc)
			filtered_specc = dictionary[specc]
		#	st.write(filtered_specc)
			filtered_set = set(filtered_specc)
			if n == 1:
				common_ids_original = filtered_set
			else:
				common_ids_original = common_ids_original & filtered_set
			n = n + 1
#			st.write(common_ids_original)
#			st.write(type(common_ids_original))
		if nom == 1:
			common_ids = common_ids_original
		else:
			common_ids = common_ids | common_ids_original
		#st.write(common_ids)
		nom = nom + 1
	filtered = []
	for sp in species:
		query = (df.filter(pl.col("Common_Name").is_in(species)).collect())
		columns_to_select = ["Place", "Checklist_ID", "Observation_Date", "State", "County"]
		available_columns = [col for col in columns_to_select if col in query.columns]
		query = query.select(available_columns)
		filtered.append(query)
		del(query)
		gc.collect()
	st.write(combonotions)
#this is what happens if it is the same, e.g., 6 out of 6 species
else: 
	filtered = []
	for sp in species:
		#st.write('starting')
		
		query = (df.filter(pl.col("Common_Name") == sp).collect())
		
		columns_to_select = ["Place", "Checklist_ID", "Observation_Date", "State", "County"]
		
		available_columns = [col for col in columns_to_select if col in query.columns]
		query = query.select(available_columns)
		
		filtered.append(query)
		del(query)
		gc.collect()
	#st.write(filtered)
	#st.write(f["Checklist_ID"])
	ids = [set(f["Checklist_ID"].to_list()) for f in filtered]
	

	common_ids = set.intersection(*ids)
	

			
#		common_ids = common_ids | common_ids_original
#	st.write(common_ids)
		
#	ids = [set(f["Checklist_ID"].to_list()) for f in filtered]
#	common_ids = set.intersection(*ids)

#st.write(filtered)
#st.write(type(filtered))
#st.write(filtered[0].unique())
	
#st.write('ending')



#filtering
result = filtered[0].filter(pl.col("Checklist_ID").is_in(common_ids)).unique()
del(filtered)
del(common_ids)
gc.collect()


#st.write('or here?')


result = filter_by_date_range(df = result, start_date_str = str(start_date), end_date_str = str(end_date))



#st.write('here?')

new_place = input_file[:-8]

if new_place != place_original:
	length = place_original.split('-')
	level = len(length)
#	st.write(level)
	if level == 3:
		result = result.filter(pl.col("County") == place_original)
	if level == 2:
		result = result.filter(pl.col("State") == place_original)
#		st.write("Place_original must be messed up")

result = result.select("Place", "Checklist_ID", "Observation_Date")

#st.write('before trytry')




result_try = result.group_by(["Place", "Observation_Date"]).len()
#st.write(result_try)
tryrty = result_try["Place"].value_counts()
tryrty = tryrty.sort("count", descending=True)
bad_places_df = tryrty.filter(pl.col("count") == 1)
#st.write(bad_places)
bad_places = bad_places_df['Place'].to_list()
#st.write(bad_places)
#st.write(tryrty)

#st.write('after trytry')



result = result.filter(~pl.col("Place").is_in(bad_places))

st.write_stream(stream_data_ca())

#st.write('before placeval')


lazy_df = df.unique(subset=["Checklist_ID"])

del df

gc.collect()

checklist_placeval = lazy_df.select("Place", "Observation_Date").collect()

checklist_placeval = filter_by_date_range(df = checklist_placeval, start_date_str = str(start_date), end_date_str = str(end_date))

place_counts = checklist_placeval["Place"].value_counts()

del(lazy_df)
del(checklist_placeval)

gc.collect()


#st.write('after placecounts')


result_placeval = result["Place"].value_counts()
del(result)
gc.collect()

#st.write(result_placeval.head(15))

result_placeval = result_placeval.filter(pl.col("count") > 1)

#st.write('after resultcounts')


col1, col2 = st.columns([5,7])

# Compute co-occurrence rate (percentage of checklists at place that include all species)
placeval_df = place_counts.join(result_placeval, on="Place", how="left").fill_null(0)
#st.write(placeval_df.head(15))

#st.write('starting to calculate percents')


placeval_df = placeval_df.with_columns((
	(pl.col("count_right") / pl.col("count") * 100).round(2).alias("Co-occurrence Rate")
))

placeval_df = placeval_df.filter(pl.col("count_right") > 0)

top_results = result_placeval.sort("count", descending=True).head(10)

top_results_percents = place_counts.join(top_results, on="Place", how="right").fill_null(0)
top_results_percents = top_results_percents.with_columns((
	(pl.col("count_right") / pl.col("count") * 100).round(2).alias("Co-occurrence Rate")
))

del(place_counts)
gc.collect()

#st.write(top_results_percents)
top_results_percents = top_results_percents.select(["Place", "Co-occurrence Rate", "count_right"])
#st.write(top_results_percents)
top_cocurrance = placeval_df.sort("Co-occurrence Rate", descending=True).select(["Place", "Co-occurrence Rate", "count_right"])
top_cocurrance = top_cocurrance.filter(pl.col("count_right") >= int(min_check))
top_cocurrance = top_cocurrance.head(20)
#st.write(top_cocurrance)

#st.write('done')


final_df = top_results_percents.vstack(top_cocurrance).unique(subset=["Place"])

#st.write('stacked')


result_placeval = result_placeval.rename({"count": "Count"})

# st.write top 15 raw co-occurrence counts
with col1:
	st.write_stream(stream_data())
	st.write(result_placeval.sort("Count", descending=True).head(30))

final_df = final_df.rename({"count_right": "Count"})
final_df = final_df.filter(pl.col("Count") >= int(min_check))

with col2:
	st.write_stream(stream_data_1())
	st.write(final_df.sort("Co-occurrence Rate", descending=True).select(["Place", "Co-occurrence Rate", "Count"]))
	st.caption("\n Anything with less than " + str(min_check) + " reports at a location are not included.")
	st.caption("\n This includes the top 10 results from the raw occurance counts to the left (if they have enough reports).")

st.write_stream(stream_data_2())
st.write_stream(stream_data_cit())



if "df" in st.session_state:
    del st.session_state["df"]
st.cache_data.clear()

#snapshot = tracemalloc.take_snapshot()
#top_stats = snapshot.statistics('lineno')

#st.write(" Top 10 memory-consuming lines ")
#for stat in top_stats[:10]:
#	st.write(stat)

#tracemalloc.stop()
