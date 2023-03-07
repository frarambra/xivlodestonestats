import requests
import utils
import os
from pymongo import MongoClient


# FFLogs related "constants"
CLIENT_ID = os.getenv("FFLOGS_CLIENT_ID")
CLIENT_SECRET = os.getenv("FFLOGS_CLIENT_SECRET")
AUTH_URL = "https://www.fflogs.com/oauth/authorize"
TOKEN_URL = "https://www.fflogs.com/oauth/token"
API_URL = "https://www.fflogs.com/api/v2/client"

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

fflogs_current_tier = '''{{
    characterData {{
        character({}) {{
            lodestoneID {} zoneRankings
        }}
    }}
}}
'''

fflogs_old_tiers = '''{{
    characterData {{
        character({}) {{
            lodestoneID {}
            {}
        }}
    }}
}}
'''

points_info_query = '''{
    rateLimitData{ limitPerHour, pointsSpentThisHour, pointsResetIn}
}'''


def get_fflogs_token():
    token_res = requests.post(TOKEN_URL, data={'grant_type': 'client_credentials'},
                              verify=False, allow_redirects=False,
                              auth=(CLIENT_ID, CLIENT_SECRET))
    return token_res.json()


def create_metadata_collections():
    mongo_client = MongoClient(utils.MONGO_URI)
    db = mongo_client[utils.DATABASE]
    # generate data regarding raid zones from fflogs API
    tokens = get_fflogs_token()
    headers = {'Content-Type': "application/json", 'Authorization': 'Bearer ' + tokens['access_token']}
    fflogs_res = requests.post(API_URL, headers=headers, json={'query': zone_query})
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

    fflogs_res = requests.post(API_URL, headers=headers, json={'query': world_query})
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
