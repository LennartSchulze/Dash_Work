import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from colorama import Fore, Style
from pathlib import Path
import os
from google.cloud import storage
import json
import pandas_gbq


app = FastAPI()

# Allowing all middleware is optional, but good practice for dev purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

@app.get("/")
def root():
    return {"status" :"ok"}



### FUNCTION TO GET GROUPED DATA FOR MAP COLORING
@app.get("/maps")
def get_map_data(
         geo_level:str,
         data_has_header=True
     ):
    if geo_level=="landkreis":
        query = f"""SELECT count(landkreis_georef) as num_jobs, landkreis_georef
        FROM `carbon-dominion-384010.master_all_jobs.AllJobs1`
        GROUP BY landkreis_georef
        LIMIT 420"""
        client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))
        query_job = client.query(query)
        result = query_job.result()
        map_data = result
    if geo_level=="bundesland":
        query = f"SELECT count(*) as num_jobs, bundesland FROM `carbon-dominion-384010.master_all_jobs.AllJobs1` GROUP BY bundesland"
        client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))
        query_job = client.query(query)
        result = query_job.result()
        map_data = result
    return map_data


# @app.get("/top_5_employers/")
# def get_top_5_employees (grouper_var: str, filter_var: str):
#     if grouper_var == 'landkreis':
#         grouper_var = 'landkreis_georef'

#     filter_var = str(filter_var)

#     query = f"""
#         SELECT arbeitgeber, COUNT(arbeitgeber) as refnr
#         FROM `carbon-dominion-384010.master_all_jobs.AllJobs1`
#         WHERE {grouper_var}='{filter_var}'
#         GROUP BY arbeitgeber
#         """

#     client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))
#     query_job = client.query(query)
#     result = query_job.result()
#     df_filtered_employer = result
#     return {"result": df_filtered_employer}


# @app.get("/top_5_branchengruppe/")
# def get_top_5_branchengruppe (grouper_var: str, filter_var: str):

#     if grouper_var == 'landkreis':
#         grouper_var = 'landkreis_georef'

#     filter_var = str(filter_var)

#     query = f"""
#         SELECT branchengruppe, COUNT(branchengruppe) as refnr
#         FROM `carbon-dominion-384010.master_all_jobs.AllJobs1`
#         WHERE {grouper_var}='{filter_var}'
#         GROUP BY branchengruppe
#         """

#     client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))
#     query_job = client.query(query)
#     result = query_job.result()
#     df_filtered_branchengruppe = result
#     return {"result": df_filtered_branchengruppe}




# @app.get("/pub_date/")
# def get_pub_date (grouper_var: str, filter_var: str):

#     if grouper_var == 'landkreis':
#         grouper_var = 'landkreis_georef'

#     filter_var = str(filter_var)

#     query = f"""
#         SELECT aktuelleVeroeffentlichungsdatum, COUNT(aktuelleVeroeffentlichungsdatum) as refnr
#         FROM `carbon-dominion-384010.master_all_jobs.AllJobs1`
#         WHERE {grouper_var}='{filter_var}'
#         GROUP BY aktuelleVeroeffentlichungsdatum
#         """

#     client = bigquery.Client(project=os.environ.get("GCP_PROJECT"))
#     query_job = client.query(query)
#     result = query_job.result()
#     df_filtered_pup_date = result
#     return {"result": df_filtered_pup_date}
