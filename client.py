import asyncio
import aiohttp
import datetime
import json
import traceback
import utils

from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pprint import pprint


# TODO: rewrite update part for mongo
class Client:
    def __init__(self, chunk_len=13):
        self.mongo_client = MongoClient(utils.MONGO_SERVER)
        self.db = self.mongo_client[utils.DATABASE]
        self.raids = self.db[utils.endgame_metadata].find_one({"raids": {"$exists": True}})['raids']
        self.chunk_len = chunk_len
        self.err_list = []
        self.err_lock = asyncio.Lock()
        self.update_op_list = []
        self.chara_lock = asyncio.Lock()
        tmp = utils.get_fflogs_token()
        self.fflogs_token = tmp['access_token']
        self.tokes_ttl = tmp['expires_in']

    async def main_process(self):
        reader, writer = await asyncio.open_connection(
            '127.0.0.1', 8888)
        print('client: connection opened')
        await asyncio.sleep(3)
        async with aiohttp.ClientSession() as session:
            try:
                while True:
                    # do not ask for more indexes if there's already a lot to request again
                    if len(self.err_list) <= self.chunk_len:
                        request = json.dumps({"request_type": "req_lode_index", "chunk_len": self.chunk_len})
                        writer.write(f'{request}\n'.encode())
                        raw_data = await reader.readline()
                        message = json.loads(raw_data.decode())
                        indexes_to_scrap = message["indexes"] if not self.err_list else self.err_list+message["indexes"]
                    else:
                        indexes_to_scrap = self.err_list
                    for tmp in utils.split(indexes_to_scrap, self.chunk_len):
                        await asyncio.gather(*[asyncio.create_task(self.scrap_character(session, _))
                                               for _ in tmp])

                    self.db['lodestone'].bulk_write(self.update_op_list)
                    self.update_op_list = []
            except:
                print(traceback.print_exc())
            finally:
                writer.close()
                await writer.wait_closed()

    async def scrap_character(self, session, character_id=None):
        character_info = {'_id': character_id}  # 'last_checked': datetime.datetime.now()}
        url = f'https://eu.finalfantasyxiv.com/lodestone/character/{character_id}/'
        async with session.get(url) as response:
            if response.status == 200:
                character_page = await response.text()
                character_info = {**character_info, **self.get_character_info(character_page),
                                  'exists': 'S', 'scrapped_date': datetime.datetime.now()}
            elif response.status == 404:
                character_info['exists'] = 'N'
            # Lodestone returns 429 if there's too many request from a single endpoint
            elif response.status == 429:
                print(f'Err 429 on {character_id}')
                async with self.err_lock:
                    self.err_list.append(character_id)
            else:
                character_info['error'] = response.status
                character_info['webpage'] = await response.text()
                print(f'There was an error with character {character_id}')

        async with self.chara_lock:
            operation = UpdateOne({"_id": character_info["_id"]}, {"$set": character_info}, upsert=True)
            self.update_op_list.append(operation)

    @staticmethod
    def get_character_info(character_page: str) -> dict:
        tmp = {}
        soup = BeautifulSoup(character_page, 'html.parser')
        tmp['name'] = soup.find('p', class_='frame__chara__name').text
        title_tag = soup.find('p', class_='frame__chara__title')
        if title_tag:
            tmp['title'] = soup.find('p', class_='frame__chara__title').text
        world_dc = soup.find('p', class_='frame__chara__world').text
        tmp['world'], tmp['datacenter'] = world_dc.split(' ')
        tmp['datacenter'] = tmp['datacenter'].replace('[', '').replace(']', '')
        # Not all characters are in a free company
        a_tag = soup.find('div', class_='character__freecompany__name').find('a')
        # -2 due to the / at the end of the url
        tmp['fc_id'] = a_tag.get('href').split('/')[-2] if a_tag else None
        tmp['jobs'] = {
            li.img.get('data-tooltip').replace(' (Limited Job)', '').split(' / ')[0]:
                0 if li.text == '-' else int(li.text)
            for li in [x for y in soup.find_all('div', class_='character__level__list') for x in y.find_all('li')]
        }

        return tmp

    async def get_fflogs_info(self, session: aiohttp.ClientSession, fflogs_id=None, character_data=None):
        character_filter = f'id: {fflogs_id}' if fflogs_id else \
            f'name: "{character_data["name"]}" serverSlug: "{character_data["server"]}" ' +\
            f'serverRegion: "{character_data["region"]}"'
        raids = ','.join([f'e_{_}: zoneRankings(includePrivateLogs: true, zoneID: {_})' for _ in self.raids.keys()])
        # format strings are dumb for {}
        query = f'{{characterData {{character({character_filter}){{name, lodestoneID, hidden, id, {raids}}}}}}}'
        headers = {'Content-Type': "application/json", 'Authorization': f'Bearer {self.fflogs_token}'}
        payload = {'query': query}

        async with session.post(utils.fflogs_vars['api_url'], headers=headers, json=payload) as response:
            response_data = await response.json()
            response_data = response_data["data"]["characterData"]["character"]
            clean_data = {"name": response_data["name"], "_id": response_data["lodestoneID"],
                          "hidden": response_data["hidden"], "fflogs_id": response_data["id"],
                          "raids": {**{key.replace('e_', ''): value
                                       for key, value in response_data.items() if 'e_' in key}}}
            pprint(clean_data)
