from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
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


def get_map(grouper_var, sector="All Sectors"):
    if sector == "All Sectors":
        query = f"""
            SELECT count({grouper_var}) as num_jobs, {grouper_var}
            FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
            GROUP BY {grouper_var}
            """
        df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015',
                         credentials=credentials)
    else:
        query = f"""SELECT count({grouper_var}) as num_jobs, {grouper_var}
            FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
            WHERE branchengruppe='{sector}'
            GROUP BY {grouper_var}
            """
        df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)
    return df


def top_5_employers(grouper_var, filter_var):
    query = f"""
        SELECT arbeitgeber, COUNT(arbeitgeber) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
        WHERE {grouper_var}='{filter_var}'
        GROUP BY arbeitgeber
        ORDER BY refnr DESC
        LIMIT 5
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015',
                     credentials=credentials)
    return df


def top_5_branchengruppe(grouper_var, filter_var):
    query = f"""
        SELECT branchengruppe, COUNT(branchengruppe) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
        WHERE {grouper_var}='{filter_var}'
        GROUP BY branchengruppe
        ORDER BY refnr DESC
        LIMIT 5
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015',
                     credentials=credentials)
    return df


def pub_date(grouper_var, filter_var):
    query = f"""
        SELECT aktuelleVeroeffentlichungsdatum,
        COUNT(aktuelleVeroeffentlichungsdatum) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
        WHERE {grouper_var}='{filter_var}'
        GROUP BY aktuelleVeroeffentlichungsdatum
        ORDER BY aktuelleVeroeffentlichungsdatum
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015',
                     credentials=credentials)
    return df


def company_sizes(grouper_var, filter_var):
    filter_var = str(filter_var)
    query = f"""
        SELECT betriebsgroesse, COUNT(betriebsgroesse) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs_wow
        WHERE {grouper_var}='{filter_var}'
        GROUP BY betriebsgroesse
        ORDER BY betriebsgroesse ASC
        """
    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015', credentials=credentials)
    return df


@app.get("/")
def root():
    return {'Hello!': 'You are an amazing person <3'}


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
def get_map_info(grouper_var: str, sector: str):
    df = get_map(grouper_var, sector)
    return {"result": df.to_json()}


@app.get("/company_size")
def get_temp_work(grouper_var: str, filter_var: str):
    df = company_sizes(grouper_var, filter_var)
    return {"result": df.to_json()}
