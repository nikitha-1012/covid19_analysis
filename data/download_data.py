import os
from shutil import rmtree
from time import sleep

import pandas as pd
import requests
import wbdata

def delete_directory(path):
    """
    Remove directory.
    """
    try:
        rmtree(path)
    except FileNotFoundError:
        pass    

def download_covid():
    """
    Download COVID-19 case data directly from JHU CSSE repository.
    """
    base_url = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
    files = [
        'time_series_covid19_confirmed_global.csv',
        'time_series_covid19_recovered_global.csv',
        'time_series_covid19_deaths_global.csv'
    ]
    path = './data/raw/COVID-19/csse_covid_19_data/csse_covid_19_time_series'
    os.makedirs(path, exist_ok=True)

    print('Downloading covid data.')
    for filename in files:
        target_path = os.path.join(path, filename)
        if not os.path.exists(target_path):
            res = requests.get(base_url + filename, timeout=30)
            res.raise_for_status()
            with open(target_path, 'wb') as f:
                f.write(res.content)
            print(f'  Saved {filename}')
        else:
            print(f'  {filename} already exists.')

def download_countries():
    """
    Download countries csv.
    """
    url = 'https://pkgstore.datahub.io/JohnSnowLabs/country-and-continent-codes-list/country-and-continent-codes-list-csv_csv/data/b7876b7f496677669644f3d1069d3121/country-and-continent-codes-list-csv_csv.csv'
    path = './data/raw/datahub'
    os.makedirs(path, exist_ok=True)

    print('Downloading country data.')
    target_path = os.path.join(path, 'countries.csv')
    if not os.path.exists(target_path):
        req = requests.get(url, timeout=30)
        req.raise_for_status()
        with open(target_path, 'wb') as csv_file:
            csv_file.write(req.content)
        print('  Saved countries.csv')
    else:
        print('  countries.csv already exists.')

def download_world_bank():
    """
    Download data from the World Bank
    """
    path = './data/raw/world_bank'
    os.makedirs(path, exist_ok=True)
    os.makedirs('./data/processed', exist_ok=True)

    # Generate world_bank_codes.csv
    codes_path = './data/processed/world_bank_codes.csv'
    if not os.path.exists(codes_path):
        print('Generating world_bank_codes.csv...')
        wb_countries = wbdata.get_countries()
        codes_df = pd.DataFrame([{'Country Code': c['id'], 'Country Name': c['name']} for c in wb_countries])
        codes_df.to_csv(codes_path, index=False)
        print('  Saved world_bank_codes.csv')

    indicators = [{'NY.GDP.PCAP.PP.CD': 'GDP per capita, PPP (current international $)'},
                  {'SP.POP.TOTL': 'Population, total'},
                  {'SP.URB.TOTL.IN.ZS': 'Urban population (% of total population)'},
                  {'EN.POP.SLUM.UR.ZS': 'Slum population (% of urban population)'},
                  {'SP.RUR.TOTL.ZS': 'Rural population (% of total population)'},
                  {'SP.DYN.LE00.IN': 'Life expectancy at birth, total (years)'},
                  {'SH.XPD.CHEX.GD.ZS': 'Current health expenditure (% of GDP)'}]

    for indicator in indicators:
        file_name = list(indicator.keys())[0]
        full_path = f'{path}/{file_name}.csv'

        if os.path.exists(full_path):
            print(f'  {file_name}.csv already exists.')
            continue

        print(f'Downloading {indicator}...')        
        try:
            df = wbdata.get_dataframe(indicator)
            df.to_csv(full_path)
            print(f'  Saved {file_name}.csv')
            sleep(1)
        except Exception as e:
            print(f'  Download failed for {indicator}: {e}')

if __name__ == '__main__':
    download_covid()    
    download_countries()
    download_world_bank()