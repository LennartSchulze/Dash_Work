import datetime
import os
import pathlib
import requests
import zipfile
import pandas as pd
import geopandas as gpd
import streamlit as st
import folium
from streamlit_folium import st_folium
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import plotly.express as px

st.set_page_config(layout="wide")

#### SIDE BAR ####

## INFORMATION ON PROJECT ##
st.sidebar.title("Dash/Work")
st.sidebar.write(
    '''
    This is a Dashboard to get an overview on the  labor market  in German regions. Based on currently open job postings, we calculate a score to measure the status of the labor market for each German district and city.
    ''')
st.sidebar.write("Find our team and contact information here: [GitHub Repository](https://github.com/LennartSchulze/Dash_Work)")

## END OF INFORMATION ON PROJECT ##

## OPTIONS ##
st.sidebar.title("Options")

geo_level_options = ["Districts and Cities", "Bundeslaender"]
geo_level_default = geo_level_options.index("Districts and Cities") #set default
geo_level = st.sidebar.selectbox("Choose geographical level", options=geo_level_options, index=geo_level_default)

sector_options = ["All Sectors", "Sector1","Sector2"]
sector_default = sector_options.index("All Sectors") #set default
sector = st.sidebar.selectbox("Focus on a specific sector", options=sector_options, index=sector_default)

company_size_options = ["All Companies", "0-5 employees","6-20 employees", "20-200 employees","more than 200 employees"]
company_size_default = company_size_options.index("All Companies") #set default
company_size = st.sidebar.selectbox("Focus on a specific company size", options=company_size_options, index=company_size_default)

skill_level_options = ["All Levels", "University Education", "Vocational Training", "No Training required"]
skill_level_default = skill_level_options.index("All Levels")
skill_level = st.sidebar.selectbox("Focus on a specific skill level", options=skill_level_options, index=skill_level_default)
## END OF OPTIONS ##

#### END OF SIDEBAR ####


#### BODY ####
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.title("Open positions in Germany")

col1, col2 = st.columns(2)

### Column 1

with col1:

    ## CHOOSE DATA BASE FOR MAP ##
    chosen_map = f"{geo_level_options.index(geo_level)}{sector_options.index(sector)}{company_size_options.index(company_size)}{skill_level_options.index(skill_level)}"
    ## END OF CHOOSE DATA BASE FOR MAP ##

    ## GET THE GEO DATA FOR  THE MAP ##
    @st.cache_data()
    def get_map(chosen_map):
        if chosen_map == "0000":
            loadfile = "counties"
        if chosen_map =="1000":
            loadfile = "states"
        gdf = gpd.read_file(f"{loadfile}.geo.json")
        return gdf

    gdf = get_map(chosen_map)
    ## END OF GETTING GEO DATA FOR THE MAP ##

    ## GET DATA FROM MASTER JOBS FILE ##
    @st.cache_data()
    def get_data():
        df = pd.read_csv("MOCK_MASTER_ALL_JOB.csv", sep=";")
        return df

    df = get_data()
    ## END OF GET MASTER FILE DATA

    ## GET SCORE - CURRENTLY: ABSOLUTE NUMBER OF JOBS ONLINE PER GEO UNIT ##
    if geo_level == "Bundeslaender":
        grouper_var = "bundesland"
        dropper_var = "landkreis"
    else:
        grouper_var = "landkreis"
        dropper_var = "bundesland"

    jobs_online = df.groupby(grouper_var).count().drop(columns=["arbeitgeber", "publication_date","hashId", "zip", dropper_var])
    jobs_online = jobs_online.reset_index()


    gdf.index = gdf["name"]
    gdf[grouper_var] = gdf["name"]


    m = folium.Map(location=[51.1657, 10.4515], tiles="cartodbpositron", zoom_start=5.5, width=500, height=400)

    gjson = folium.Choropleth(
        geo_data=gdf,
        name="choropleth",
        data=jobs_online,
        columns=[grouper_var, "refnr"],
        key_on="feature.id",
        fill_color="Blues",
        fill_opacity=0.7,
        line_opacity=1,
        legend_name="Employment Status",
    ).add_to(m)

    #Add Customized Tooltips to the map
    folium.features.GeoJson(
                        data=gdf,
                        name='New Jobs Online',
                        smooth_factor=2,
                        style_function=lambda x: {'color':'black','fillColor':'transparent','weight':0.5},
                        tooltip=folium.features.GeoJsonTooltip(
                            fields=[grouper_var,
                                    ],
                            aliases=[" ",
                                ],
                            localize=True,
                            sticky=False,
                            labels=True,
                            style="""
                                background-color: #F0EFEF;
                                border: 2px solid black;
                                border-radius: 3px;
                                box-shadow: 3px;
                            """,
                            max_width=100,),
                                highlight_function=lambda x: {'weight':3,'fillColor':'grey'},
                            ).add_to(m)


    #folium.LayerControl().add_to(m) #UNNECESSARY IF WE ONLY HAVE ONE MEANINGFUL LAYER - MAYBE CUT + Sometimes Buggy

    #output = st_folium(
    #    m, width=700, height=500, returned_objects=["last_object_clicked"]
    #    )


    output = st_folium(m, returned_objects=["last_object_clicked"], width=600, height=600)

    if output["last_object_clicked"] is not None:
        punkt = Point(output["last_object_clicked"]["lng"], output["last_object_clicked"]["lat"])

        for i in range(len(gdf.geometry)):
            polygon = gdf.geometry[i]
            if polygon.contains(punkt):
                st.write("The point you clicked on is in", gdf.name[i])
                filter_var = gdf.name[i]

        if grouper_var=="bundesland":
            df_filtered = df[df["bundesland"]==filter_var]
        if grouper_var=="landkreis":
            df_filtered = df[df["landkreis"]==filter_var]

        df_filtered_employer = df_filtered.groupby("arbeitgeber").count()
        df_filtered_employer = df_filtered_employer.sort_values("refnr", ascending=False)
        df_filtered_employer = df_filtered_employer.iloc[0:5]
        df_filtered_employer = df_filtered_employer.reset_index()

with col2:
    if filter_var is not None:
        with st.container():
            st.write(f"""<div class='cards'/><b>{filter_var}</b><br>
                    Open jobs: {len(df_filtered.index)}</div>""", unsafe_allow_html=True)

        with st.container():
            st.write(f"""<b>Employers with most job offers</b>""", unsafe_allow_html=True)
            plot_employer = px.histogram(df_filtered_employer, x="arbeitgeber", y="refnr", width=450, height=350)
            plot_employer.update_layout(
            paper_bgcolor="#EFF2F6",
            plot_bgcolor="#EFF2F6",
            xaxis_title=None,
            yaxis_title=None,

            )
            plot_employer.update_traces(
                marker_color="#09316B"
            )

            plot_employer.add_annotation(
            x="arbeitgeber",
            xref="x",
            yref="y",
            font=dict(
                family="Courier New, monospace",
                size=16,
                color="#ffffff"
                ),
            )

            st.plotly_chart(plot_employer)

        st.write(df_filtered)
        df_filtered_pubdate = df_filtered.groupby("publication_date").count()
        df_filtered_pubdate = df_filtered_pubdate.sort_values("refnr")
        df_filtered_pubdate = df_filtered_pubdate.reset_index()
        st.write(df_filtered_pubdate)

        plot_pubdate = px.line(df_filtered_pubdate, x="publication_date", y="refnr")
        st.plotly_chart(plot_pubdate)
