import requests
import json
from pymongo import MongoClient

with open('config/mongo_conn.json') as f_read:
    conn_vars = json.load(f_read)
    MONGO_SERVER = conn_vars["MONGO_SERVER"]
    DATABASE = conn_vars["DATABASE"]
    character_collection = conn_vars["character_colletion"]
    endgame_metadata = conn_vars["endgame_metadata"]

with open('config/client_params.json') as f_read:
    fflogs_vars = json.load(f_read)

zone_query = '''{
    worldData{
        expansions{
            id, name,
            zones{id, name, difficulties{id, name}, encounters{id, name}}
        }
    }
}
'''

world_query = '''{
    worldData {
        regions {id, name, slug,
            servers(limit: 100, page: 1){ data{ id, name, slug, subregion{name}}}
        }
    }
}
'''

fflogs_basic_query = '''{{
    characterData {{
        character({}) {{name, lodestoneID, canonicalID, hidden}}
    }}
}}
'''

points_info_query = '''{
    rateLimitData{ limitPerHour, pointsSpentThisHour, pointsResetIn}
}'''


def get_fflogs_token():
    token_res = requests.post(fflogs_vars['token_url'], data={'grant_type': 'client_credentials'},
                              verify=False, allow_redirects=False,
                              auth=(fflogs_vars['client_id'], fflogs_vars['client_secret']))
    return token_res.json()


def create_metadata_collections():
    mongo_client = MongoClient(MONGO_SERVER)
    db = mongo_client[DATABASE]
    # generate data regarding raid zones from fflogs API
    tokens = get_fflogs_token()
    headers = {'Content-Type': "application/json", 'Authorization': 'Bearer ' + tokens['access_token']}
    fflogs_res = requests.post(fflogs_vars['api_url'], headers=headers, json={'query': zone_query})
    api_data = fflogs_res.json()
    expansion_data = api_data['data']['worldData']['expansions']

    # all encounters
    fights, difficulties, expansion, raid_zones, tmp = {}, {}, {}, {}, {}
    for expac in expansion_data:
        expac_id, expac_name = str(expac['id']), expac['name']
        expansion[expac_id] = expac_name
        zone_rework = {}
        for zone in expac['zones']:
            zone_id, zone_name = str(zone['id']), zone['name']
            raid_zones[zone_id] = zone_name
            zone_rework[zone_id] = {'name': zone_name, 'difficulties': {}, 'encounters': {}}
            for diff_id, diff_name in [(str(_['id']), _['name']) for _ in zone['difficulties']]:
                zone_rework[zone_id]['difficulties']['diff_id'] = diff_name
                difficulties[diff_id] = diff_name
            for encounter in zone['encounters']:
                encounter_id, encounter_name = str(encounter['id']), encounter['name']
                zone_rework[zone_id]['encounters'][encounter_id] = encounter_name
                fights[encounter_id] = encounter_name
        tmp[expac_id] = {'name': expac_name, 'zones': zone_rework}

    adapted_api_data = {'worldData': {'expansions': tmp}}

    fflogs_res = requests.post(fflogs_vars['api_url'], headers=headers, json={'query': world_query})
    api_data = fflogs_res.json()
    region_data = api_data['data']['worldData']['regions']
    # pprint(region_data)
    datacenters = {}
    for item in api_data['data']['worldData']['regions']:
        region_servers = {
            str(server['id']): {'name': server['name'], 'slug': server['slug'], 'datacenter': server['subregion']['name']}
            for server in item['servers']['data']
        }
        datacenters[str(item['id'])] = {'name': item['name'], 'slug': item['slug'], 'servers': region_servers}

    db['metadata'].insert_many([adapted_api_data, {'fights': fights},
                                {'difficulties': difficulties}, {'expansion': expansion},
                                {'raids': raid_zones}, {'regions': datacenters}])
