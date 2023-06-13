
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account
import os
from google.cloud import storage
from google.cloud import bigquery

#os.path.join(os.path.join(os.path.dirname(os.getcwd()),'key'),key_name)
key_name = 'wagon-bootcamp-384015-bc60d2550ebc.json'
credentials = service_account.Credentials.from_service_account_file(os.path.join(os.path.join(os.path.dirname(os.getcwd()),'key'),key_name))
df_detail = pd.read_gbq('select * from dash_work.job_details', credentials=credentials)
#drop columns in dataframe
df_detail=df_detail.drop(["Unnamed_0", "Unnamed_1", "branche", "eintrittsdatum", "ersteVeroeffentlichungsdatum","titel"], axis=1)
#drop duplicates
df_detail.drop_duplicates(subset=['refnr'], inplace=True)
#add timestamp of access minus today in number of days
df_detail['days_since_posting'] = (pd.to_datetime('today') -pd.to_datetime(df_detail['aktuelleVeroeffentlichungsdatum'])).dt.days
#keep only df["arbeitsort.land"] == "Deutschland"
df_detail = df_detail[df_detail["arbeitsorte_land"] == "Deutschland"]
# fill Arbeitsort_ort with "hybrid" if NaN
df_detail['arbeitsorte_ort'].fillna("regional/hybrid", inplace=True)

df_mapping =pd.read_csv('MASTER_MAPPING_attempt.csv', sep=";", encoding='latin-1')
merged_df=df_detail.merge(df_mapping, how='inner',left_on='arbeitsorte_ort', right_on='arbeitsort.ort')
merged_df = merged_df.rename(columns={'ï»¿arbeitsort.plz': 'arbeitsort_ort'})
merged_df['landkreis_georef'] = merged_df['landkreis_georef'].str.replace('Ã', 'ß')
merged_df['landkreis_georef'] = merged_df['landkreis_georef'].str.replace('Ã¼', 'ü')
merged_df['landkreis_georef'] = merged_df['landkreis_georef'].str.replace('Ã¤', "ä")
merged_df['landkreis_georef'] = merged_df['landkreis_georef'].str.replace('Ã¶', 'ö')
merged_df['bundesland'] = merged_df['bundesland'].str.replace('Ã¼', 'ü')


#send file to google cloud storage
def save_dataframe_to_bq(
        data: pd.DataFrame,
        gcp_project:str,
        bq_dataset:str,
        table: str,
        truncate: bool,
        credentials
    ) -> None:
    """
    - Save the DataFrame to BigQuery
    - Empty the table beforehand if `truncate` is True, append otherwise
    """

    assert isinstance(data, pd.DataFrame)
    full_table_name = f"{gcp_project}.{bq_dataset}.{table}"
    print(f"\nSave data to BigQuery @ {full_table_name}...:")

    # Load data onto full_table_name
    data.columns = [f"_{column}" if not str(column)[0].isalpha() and not str(column)[0] == "_" else str(column) for column in data.columns]

    client = bigquery.Client(credentials=credentials)

    # Define write mode and schema
    write_mode = "WRITE_TRUNCATE" if truncate else "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)

    print(f"\n{'Write' if truncate else 'Append'} {full_table_name} ({data.shape[0]} rows)")

    # Load data
    job = client.load_table_from_dataframe(data, full_table_name, job_config=job_config)
    result = job.result()  # wait for the job to complete

    print(f":white_check_mark: Data saved to bigquery, with shape {data.shape}")

save_dataframe_to_bq(merged_df, 'wagon-bootcamp-384015', 'dash_work', 'merged_df', truncate=True, credentials=credentials)

#df_landkreis=merged_df.groupby("landkreis_georef")['refnr'].count()
#df_bundesland=merged_df.groupby("bundesland")['refnr'].count()

#show rows that contain the word Aachen
#df_detail[df_detail['arbeitsorte_ort'].str.contains("Aachen")]
