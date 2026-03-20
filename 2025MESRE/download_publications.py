import numpy as np
import pandas as pd
import requests
import os
from tqdm.autonotebook import tqdm


def params_creator(query, year, view='COMPLETE', cursor='*'):

    # add the year to the query
    query =  "({}) AND PUBYEAR = {}".format(query, year)

    if view == 'COMPLETE':
        count = 25
    else:
        count = 200
	
    params = {
    'query': query,
    'count': count,
    'view': view,
    'cursor': cursor
    }

    return params

def make_df(list_of_queries, start_year, end_year):
    return pd.DataFrame({'query': list_of_queries, 'start_year': start_year, 'end_year': end_year, 'current_year': start_year, 'current_cursor': '*', 'page': 0})

def save_log(log_df, export_path):
    log_df.to_pickle('{}/log.pkl'.format(export_path))
    return

def download_publications(df_queries, MY_API_KEY_LIST, continue_halted=True, export_path='../data/raw/publications', sub_domain='general', view='COMPLETE', MAX_TRIALS=25, TIMEOUT=60): 
    if continue_halted:
        try:
            df_queries = pd.read_pickle('{}/log.pkl'.format(export_path))
            print('Continuing from log')
        except:
            print('No log found, will start from original dataframe')
            df_queries = df_queries.copy()
    
    else:
        df_queries = df_queries.copy()
    
    key_id = 0
    MY_API_KEY = MY_API_KEY_LIST[key_id]

    # if the path doesn't exist, create it
    if not os.path.exists(export_path):
        os.makedirs(export_path)    

    for i, row in tqdm(df_queries.iterrows(), desc='Processing queries'):

        query = row['query']

        start_year = row['start_year']
        end_year = row['end_year']
        
        current_year = row['current_year']
        current_cursor = row['current_cursor']
        
        if current_year > end_year:
            print('Skipping completed query...')
            continue
        
        page = row['page']
        
        for year in tqdm(range(current_year, end_year+1), desc='Looping through years'):
            
            first_query=True

            params = params_creator(query, year, view, current_cursor)

            while True:
                trials = 0
                while trials < MAX_TRIALS:
                    try:
                        response = requests.get(url = 'http://api.elsevier.com/content/search/scopus',
                                                headers={'Accept':'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                                params=params,
                                                timeout=TIMEOUT)
                        break
                
                    except:
                        trials+=1
                        continue
            
                if trials == MAX_TRIALS:
                    print('TIMEOUT')
                    df_queries['current_year'].iloc[i] = year
                    df_queries['current_cursor'].iloc[i] = current_cursor
                    df_queries['page'].iloc[i] = page
                    
                    save_log(df_queries, export_path)
                    return
                
                status = response.status_code

                while status == 429:
                    key_id = key_id+1
                
                    try:
                        MY_API_KEY = MY_API_KEY_LIST[key_id]
                    
                    except:
                        print('OUT OF API KEYS')
                        df_queries['current_year'].iloc[i] = year
                        df_queries['current_cursor'].iloc[i] = current_cursor
                        df_queries['page'].iloc[i] = page
                        
                        save_log(df_queries, export_path)
                        return

                    trials = 0
                    while trials < MAX_TRIALS:
                        try:
                            response = requests.get(url = 'http://api.elsevier.com/content/search/scopus',
                                                    headers={'Accept':'application/json', 'X-ELS-APIKey': MY_API_KEY},
                                                    params=params,
                                                    timeout=TIMEOUT)
                            break
                            
                        except:
                            trials+=1
                            continue
                    
                    if trials == MAX_TRIALS:
                        print('TIMEOUT')
                        df_queries['current_year'].iloc[i] = year
                        df_queries['current_cursor'].iloc[i] = current_cursor
                        df_queries['page'].iloc[i] = page

                        save_log(df_queries, export_path)
                        return
                
                status = response.status_code  

                if status in [400, 401, 403]:
                    df_queries['current_year'].iloc[i] = year
                    df_queries['current_cursor'].iloc[i] = current_cursor
                    df_queries['page'].iloc[i] = page

                    if response.status_code == 401:
                        print('Exception HTTP 401 - Unauthorized (APIKey [...] with IP address [...] is unrecognized or has insufficient privileges for access to this resource)')

                    if response.status_code == 403:
                        print('Exception HTTP 403 - Forbidden (Requestor configuration settings undefined or insufficient for access to this resource.)')
                    
                    if response.status_code == 400:
                        print(response.json())
                        print('Exception HTTP 400 - Invalid Input ({})'.format(response.json()['service-error']['status']['statusText']))
                        
                    save_log(df_queries, export_path)
                    return

                
                # Convert the response in a json format
                response_json = response.json()
                
                try:
                    results = response_json['search-results']
                except:
                    print('ERROR READING JSON')
                    df_queries['current_year'].iloc[i] = year
                    df_queries['current_cursor'].iloc[i] = current_cursor
                    df_queries['page'].iloc[i] = page

                    save_log(df_queries, export_path)
                    return
                
                if first_query:
                    total_number_records = int(results['opensearch:totalResults'])
                    print('Found {} publications. Downloading {} files...'.format(total_number_records, int(np.ceil(total_number_records/params['count']))))
                    pbar = tqdm(total=int(np.ceil(total_number_records/params['count'])))
                
                    if current_cursor!='*':
                        pbar.update(page)
                
                    first_query=False
                
                current_cursor = results['cursor']['@current']
                next_cursor = results['cursor']['@next']

                if current_cursor == next_cursor:
                    break

                request_json_entry = pd.json_normalize(results['entry'])
                # Save the results in a json file
                request_json_entry.to_json("{}/file_{}_{}_{}_{}.json".format(export_path, sub_domain, i, year, page))

                current_cursor = next_cursor

                # set the current cursor in PARAMS
                params['cursor'] = current_cursor

                page+=1
                pbar.update(1)
                
            current_cursor = '*'
            page = 0
            
            df_queries['current_cursor'].iloc[i] = current_cursor
            df_queries['current_year'].iloc[i] = year+1
            df_queries['page'].iloc[i] = page
        
    save_log(df_queries, export_path)
    return
