from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from Dash_Work.backend.data.bq_actions import *
import pandas_gbq

app = FastAPI()
#TODO: load any data files at this point already


# Allowing all middleware is optional, but good practice for dev purposes
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


app.state.df = pd.read_csv('/Users/kruasanova/code/LennartSchulze/Dash_Work/data/master_all_jobs.csv')
app.state.df["landkreis"] = app.state.df["landkreis_georef"]

def load_master_all_jobs(grouper_var, filter_var):
    if grouper_var == 'landkreis':
        grouper_var = 'landkreis_georef'

    filter_var = str(filter_var)

    query = f"""
        SELECT arbeitgeber, COUNT(arbeitgeber) as refnr
        FROM wagon-bootcamp-384015.dash_work.master_all_jobs
        WHERE {grouper_var}='{filter_var}'
        GROUP BY arbeitgeber
        """

    print (query)

    df = pd.read_gbq(query=query, project_id='wagon-bootcamp-384015')

    return df

@app.get("/")
def root():
    return {'greeting': 'This is the root directory'}


@app.get("/top_5_employers/")
def get_top_5_employees (grouper_var: str, filter_var: str):
    # df_filtered = app.state.df[app.state.df[grouper_var] == filter_var]
    # df_filtered_employer = df_filtered.groupby("arbeitgeber").count()
    # df_filtered_employer = df_filtered_employer.sort_values("refnr", ascending=False)
    # df_filtered_employer = df_filtered_employer.iloc[0:5]
    # df_filtered_employer = df_filtered_employer.reset_index()
    df_filtered_employer = load_master_all_jobs(grouper_var, filter_var)
    return {"result": df_filtered_employer.to_json()}


@app.get("/top_5_branchengruppe/")
def get_top_5_branchengruppe (grouper_var: str, filter_var: str):
    df_filtered = app.state.df[app.state.df[grouper_var] == filter_var]
    df_filtered_branchengruppe = df_filtered.groupby("branchengruppe").count()
    df_filtered_branchengruppe = df_filtered_branchengruppe.sort_values("refnr", ascending=False)
    df_filtered_branchengruppe = df_filtered_branchengruppe.iloc[0:5]
    df_filtered_branchengruppe = df_filtered_branchengruppe.reset_index()

    return {"result": df_filtered_branchengruppe.to_json()}


@app.get("/pub_date/")
def get_pub_date (grouper_var: str, filter_var: str):
    df_filtered = app.state.df[app.state.df[grouper_var] == filter_var]
    df_filtered_pubdate = df_filtered.groupby("aktuelleVeroeffentlichungsdatum").count()
    df_filtered_pubdate = df_filtered_pubdate.sort_values("refnr")
    df_filtered_pubdate = df_filtered_pubdate.reset_index()
    df_filtered_pubdate = df_filtered_pubdate.sort_values(by="aktuelleVeroeffentlichungsdatum")

    return {"result": df_filtered_pubdate.to_json()}
