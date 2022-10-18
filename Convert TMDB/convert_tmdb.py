#!/usr/bin/env python3
import simplejson as json
import pandas

with open("series_list.json", "r") as series_list:
    series_list_json = json.loads(series_list.read())
series = pandas.json_normalize(series_list_json)
del series["created_by"]
del series["episode_run_time"]
del series["genres"]
del series["languages"]
del series["networks"]
del series["origin_country"]
del series["production_companies"]
del series["seasons"]
series.to_csv("tsv/series.tsv", index=False, sep="\t")

series_created_by = pandas.json_normalize(series_list_json, record_path=["created_by"])
series_episode_run_time = pandas.json_normalize(series_list_json, record_path=["episode_run_time"])
series_genres = pandas.json_normalize(series_list_json, record_path=["genres"])
series_languages = pandas.json_normalize(series_list_json, record_path=["languages"])
series_networks = pandas.json_normalize(series_list_json, record_path=["networks"])
series_origin_country = pandas.json_normalize(series_list_json, record_path=["origin_country"])
series_production_companies = pandas.json_normalize(series_list_json, record_path=["production_companies"])
series_seasons = pandas.json_normalize(series_list_json, record_path=["seasons"])

with open("movies_list.json", "r") as movies_list:
    movies_list_json = json.loads(movies_list.read())
movies = pandas.json_normalize(movies_list_json)
del movies["genres"]
del movies["production_companies"]
del movies["production_countries"]
del movies["spoken_languages"]
movies.to_csv("tsv/movies.tsv", index=False, sep="\t")

movies_genres = pandas.json_normalize(movies_list_json, record_path=["genres"])
movies_production_companies = pandas.json_normalize(movies_list_json, record_path=["production_companies"])
movies_production_countries = pandas.json_normalize(movies_list_json, record_path=["production_countries"])
movies_spoken_languages = pandas.json_normalize(movies_list_json, record_path=["spoken_languages"])

genres = pandas.concat([series_genres, movies_genres], ignore_index=True)
production_companies = pandas.concat([series_production_companies, movies_production_companies], ignore_index=True)

series_created_by.drop_duplicates().to_csv("tsv/created_by.tsv", index=False, sep="\t")
series_episode_run_time.drop_duplicates().to_csv("tsv/episode_run_time.tsv", index=False, sep="\t")
genres.drop_duplicates().to_csv("tsv/genres.tsv", index=False, sep="\t")
series_languages.drop_duplicates().to_csv("tsv/languages.tsv", index=False, sep="\t")
series_networks.drop_duplicates().to_csv("tsv/networks.tsv", index=False, sep="\t")
series_origin_country.drop_duplicates().to_csv("tsv/origin_country.tsv", index=False, sep="\t")
production_companies.drop_duplicates().to_csv("tsv/production_companies.tsv", index=False, sep="\t")
series_seasons.drop_duplicates().to_csv("tsv/seasons.tsv", index=False, sep="\t")
movies_production_countries.drop_duplicates().to_csv("tsv/production_countries.tsv", index=False, sep="\t")
movies_spoken_languages.drop_duplicates().to_csv("tsv/spoken_languages.tsv", index=False, sep="\t")
