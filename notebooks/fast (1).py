from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from pathlib import Path
from Dash_Work.params import LOCAL_PROJECT_PATH
from google.oauth2 import service_account
import os

app = FastAPI()
pathdir = os.path.dirname(os.path.abspath(__file__))
private_key = os.path.join(pathdir, '..', '..', '..', 'key.json')

credentials = service_account.Credentials.from_service_account_file(
    private_key
)

# Allowing all middleware is optional, but good practice for dev purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


def get_map_bq(grouper_var):
    if grouper_var == 'landkreis':
        grouper_var = 'landkreis_georef'

    query = f"""SELECT count({grouper_var}) as num_jobs, {grouper_var}
    FROM wagon-bootcamp-384015.dash_work.master_all_jobs GROUP BY {grouper_var}
        """

    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)

    return df

def top_5_employers(grouper_var, filter_var):
    if grouper_var == 'landkreis':
        grouper_var = 'landkreis_georef'

    filter_var = str(filter_var)

    query = f"""
        SELECT arbeitgeber, COUNT(arbeitgeber) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs
        WHERE {grouper_var}='{filter_var}'
        GROUP BY arbeitgeber
        ORDER BY refnr DESC
        LIMIT 5
        """

    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)

    return df

def top_5_branchengruppe(grouper_var, filter_var):
    if grouper_var == 'landkreis':
        grouper_var = 'landkreis_georef'
    filter_var = str(filter_var)

    query = f"""
        SELECT branchengruppe, COUNT(branchengruppe) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs
        WHERE {grouper_var}='{filter_var}'
        GROUP BY branchengruppe
        ORDER BY refnr DESC
        LIMIT 5
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)

    return df

def pub_date(grouper_var, filter_var):
    if grouper_var == 'landkreis':
        grouper_var = 'landkreis_georef'
    filter_var = str(filter_var)

    query = f"""
        SELECT aktuelleVeroeffentlichungsdatum, COUNT(aktuelleVeroeffentlichungsdatum) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs
        WHERE {grouper_var}='{filter_var}'
        GROUP BY aktuelleVeroeffentlichungsdatum
        ORDER BY aktuelleVeroeffentlichungsdatum
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)

    return df

@app.get("/")
def root():
    return {'greeting': 'This is the root directory'}


@app.get("/top_5_employers/")
def get_top_5_employees(grouper_var: str, filter_var: str):
    df_filtered = top_5_employers(grouper_var, filter_var)
    return {"result": df_filtered.to_json()}


@app.get("/top_5_branchengruppe/")
def get_top_5_branchengruppe(grouper_var: str, filter_var: str):
    df_filtered = top_5_branchengruppe(grouper_var, filter_var)

    return {"result": df_filtered.to_json()}


@app.get("/pub_date/")
def get_pub_date(grouper_var: str, filter_var: str):
    df_filtered = pub_date(grouper_var, filter_var)

    return {"result": df_filtered.to_json()}


@app.get("/maps")
def get_map_info(grouper_var: str):
    df = get_map_bq(grouper_var)
    return {"result": df.to_json()}
