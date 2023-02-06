# -*- coding: utf-8 -*-
"""
Created on Fri Nov  4 15:50:54 2022
@author: Molinero-Parejo, Ramón
Spatial metrics (vector)
List: SHDI, SEI, LEI
"""

from os import chdir
from time import time
from geopandas import read_file, sjoin
from numpy import log, inf

#-----------------------------------------------------------------------------#

def neighbours(gdf, dist):
    '''
    Function that a buffer for each parcel considering a defined distance.
    '''  
    
    # parcel type field (urban or rural)
    gdf['TYPE'] = ''
    
    # indicates parcel type (urban or rural)
    gdf.loc[gdf[ref_use] == 'vacant', 'TYPE'] = 'R'
    gdf.loc[gdf[ref_use] != 'vacant', 'TYPE'] = 'U'
    
    # generates a copy for polygons
    gdf_polygon = gdf.copy(deep=True)
    
    # generates a copy for buffers
    gdf_buffer = gdf.copy(deep=True)
    
    # generates buffers for each parcel
    gdf_buffer['geometry'] = gdf_polygon.geometry.buffer(dist)
    
    # spatial join (centroids - "inner" - buffers)
    gdf_join = sjoin(gdf_buffer, gdf_polygon, how="inner", predicate="intersects", 
                             lsuffix="", rsuffix="NB")
    
    return gdf_join
    
    
def shdi_shei(gdf, d, k):
    '''
    Function that calculates multi-scale SHDI and SHEI for each parcel 
    considering different buffer distances.
    ''' 
    
    # calculates buffers area
    gdf_buffer_area = gdf.groupby(['ID_']).agg({'AREA_NB':'sum'}).reset_index()

    # calculates area for each use within a buffer
    gdf_use_area = gdf.groupby(['ID_', sim_use + '_NB']).agg({'AREA_NB':'sum'}).reset_index()
    
    # join both df
    gdf_areas = gdf_use_area.join(gdf_buffer_area, on='ID_', how='left', rsuffix='_TOTAL')
    
    # calculates of the proportion [P and LN(P)] of each use within the buffer
    gdf_areas['P'] = gdf_areas['AREA_NB'] / gdf_areas['AREA_NB_TOTAL']
    gdf_areas['LN_P'] = gdf_areas['P'].apply(lambda x: log(x))
    
    # implements SHDI formula
    gdf_areas['SHDI_' + str(d)] = - (gdf_areas['P'] * gdf_areas['LN_P'])
    
    # indicates proportion on vacant parcels is 0    
    gdf_areas.loc[gdf_areas[sim_use + '_NB'] == '', 'SHDI_' + str(d)] = 0   
    
    # group and summarise all parcel within a same buffer
    gdf_metrics = gdf_areas.groupby(['ID_']).agg({'SHDI_' + str(d):'sum', sim_use + '_NB':'nunique'}).reset_index()
    
    # implements SHEI formula
    gdf_metrics['SHEI_' + str(d)] = gdf_metrics['SHDI_' + str(d)] / log(k)
    
    # fill Nan and Inf fields with 0
    gdf_metrics = gdf_metrics.fillna(0)
    gdf_metrics = gdf_metrics.replace([inf, -inf], 0)
    
    return gdf_metrics

#-----------------------------------------------------------------------------#
    
def lei(gdf, d):
    '''
    Function that calculates multi-scale LEI for each parcel
    considering different buffer distances.
    ''' 
    
    # calculates area for each type within a buffer
    gdf_areas = gdf.groupby(['ID_', 'TYPE_NB']).agg({'AREA_NB':'sum'}).reset_index()
    
    # calculates urban area within the buffer
    gdf_areas['URBAN'] = gdf_areas['AREA_NB']
    gdf_areas.loc[gdf_areas['TYPE_NB'] == 'R', 'URBAN'] = 0

    # calculates area for each type within a buffer
    gdf_areas = gdf_areas.groupby(['ID_']).agg({'AREA_NB':'sum', 'URBAN':'sum'}).reset_index()

    # calculates LEI
    gdf_areas['LEI_' + str(d)] = gdf_areas['URBAN'] / gdf_areas['AREA_NB']
    
    return gdf_areas

#-----------------------------------------------------------------------------#

def main():
    '''
    Main function that will set up the data and organize it so that the 
    processes are automatized
    '''
    # open vector files as GeoDataFrame
    for s in scenario_list:
        shp_parcel = s + '.shp'
        gdf_parcel = read_file(shp_parcel)
        
        # creates a df for all metrics in all scales
        df_metrics = gdf_parcel[['ID']]
        
        # iterates over a list of distances (multi-scale approach)
        for d in dist_list:
            try:
                # generates new gdf with useful fields
                gdf_copy = gdf_parcel[{'ID', 'REFCAT', 'USE_2018', sim_use, 'AREA', 'ITERATION', 'CLC_2018', 'SIOSE_2014', 'geometry'}].copy(deep=True)
                
                # calculate the number of classes
                k = len(gdf_copy[sim_use].unique())
                
                # calculate neighbourhoods in each buffer distance
                gdf_neighbours = neighbours(gdf_copy, d)
                
                # calculate SHDI and SHEI
                gdf_shdi_shei = shdi_shei(gdf_neighbours, d, k)
                
                # calculate LEI
                gdf_lei = lei(gdf_neighbours, d)
                
                # merge columns with the value metrics in one df
                df_metrics['SHDI_' + str(d)] = gdf_shdi_shei['SHDI_' + str(d)]
                df_metrics['SHEI_' + str(d)] = gdf_shdi_shei['SHEI_' + str(d)]
                df_metrics = df_metrics.merge(gdf_lei[['ID_', 'LEI_' + str(d)]], how='outer', left_on='ID', right_on='ID_')                
                df_metrics = df_metrics.drop('ID_', axis=1)
                
            except ValueError:
                print('\n---> ERROR: ' + ValueError)
                
        # replace Nan values with 0
        df_metrics = df_metrics.fillna(0)
        
        # join df with all metrics to the geometry
        gdf_metrics = gdf_copy.merge(df_metrics, how='outer', left_on='ID', right_on='ID')
        
        # fill empty values
        gdf_metrics['ITERATION'] = gdf_metrics['ITERATION'].fillna('0')
        
        # calculates types of growth
        gdf_metrics['GROWTH'] = ''
        gdf_metrics.loc[gdf_metrics['LEI_50'] == 0, 'GROWTH'] = 'Outlying'
        gdf_metrics.loc[gdf_metrics['LEI_50'] > 0, 'GROWTH'] = 'Edge expansion'
        gdf_metrics.loc[gdf_metrics['LEI_50'] > 0.5, 'GROWTH'] = 'Infilling'
        gdf_metrics.loc[gdf_metrics['ITERATION'] == '0', 'GROWTH'] = 'No growth'
        
        # export gdf to *.shp file
        file_name = 'new\\metrics_' + s + '.shp'
        gdf_metrics.to_file(file_name, crs='EPSG:25830')
        
        # export df to *.xlsx file with multiscale index
        gdf_metrics.to_excel('new\\metrics_' + s + '.xlsx')
        
#-----------------------------------------------------------------------------#
sim_use = 'SIM_USE'
ref_use = 'USE_2018'

dist_list = [25, 50, 75, 100, 200, 300, 400, 500, 600, 700, 800]

scenario_list = ['s1' ,'s2', 's3']

# save initial time
t_start = time()  

# establish the working directory
wd = 'D:'
chdir(wd)

# executing main function
if __name__ == '__main__':
    main()

# save the final time
t_finish = time()

# show the execution time elapsed
t_process = (t_finish - t_start) / 60
print('Process time: ' + str(round(t_process, 2)) + 'minutes')