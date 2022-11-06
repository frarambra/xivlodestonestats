import asyncio
import aiohttp
import datetime
import traceback
import utils
import sys

from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from pprint import pprint

character_index_api = 'http://127.0.0.1:8000/scraping/lodestone/{}'
fflogs_index_api = 'http://127.0.0.1:8000/scraping/fflogs/{}'


class Client:
    def __init__(self, chunk_len=13, testing=False):
        self.testing = testing
        self.mongo_client = MongoClient(utils.MONGO_SERVER)
        self.db = self.mongo_client[utils.DATABASE]
        self.raids = self.db[utils.endgame_metadata].find_one({"raids": {"$exists": True}})['raids']
        m_filter = {
            '$and': [{'regions': {'$exists': True}},
                     {'slug': {'$in': ['NA', 'EU', 'JP', 'OC']}}]
        }
        self.worlds = self.db[utils.endgame_metadata].find_one({'regions': {'$exists': True}})['regions']
        self.worlds = {key: value for key, value in self.worlds.items() if value['slug'] in ['EU', 'NA', 'JP', 'OC']}
        normalization = {}
        self.regions = {}
        for key, tmp in self.worlds.items():
            slug = tmp['slug']
            for _, item in tmp['servers'].items():
                normalization[item['name']] = item['slug']
                self.regions[item['slug']] = slug
        self.worlds = normalization
        self.chunk_len = chunk_len
        self.err_list = []
        self.err_lock = asyncio.Lock()
        self.update_op_list = []
        self.lock = asyncio.Lock()
        tmp = utils.get_fflogs_token()
        self.token_points = True
        self.fflogs_token = tmp['access_token']
        self.time_to_new_token = datetime.datetime.now()+datetime.timedelta(seconds=tmp['expires_in'])

    async def main_process(self):
        lodestone_session = aiohttp.ClientSession()
        fflogs_session = aiohttp.ClientSession()
        api_session = aiohttp.ClientSession()
        try:
            while True:
                # prepare the tasks
                if len(self.err_list) <= self.chunk_len:
                    async with api_session.get(character_index_api.format(self.chunk_len)) as response:
                        data = await response.json()
                        indexes_to_scrap = data['lodestone_indexes'] if not self.err_list else \
                            self.err_list + data['lodestone_indexes']
                else:
                    indexes_to_scrap = self.err_list
                print(f'characters to scrap: {indexes_to_scrap}')
                tasks = []
                for tmp in utils.split(indexes_to_scrap, self.chunk_len):
                    tasks = [asyncio.create_task(self.scrap_character(lodestone_session, _)) for _ in tmp]

                if self.token_points:
                    async with fflogs_session.get(fflogs_index_api.format(self.chunk_len)) as response:
                        data = await response.json()
                        tasks += [asyncio.create_task(self.get_fflogs_info(session=fflogs_session, fflogs_id=_))
                                  for _ in data['fflogs_id']]
                        tasks += [asyncio.create_task(self.get_fflogs_info(session=fflogs_session, character_data=_))
                                  for _ in data['character_data']]
                # execute the tasks
                await asyncio.gather(*tasks)
                # update info on mongo
                if self.update_op_list:
                    self.db[utils.character_collection].bulk_write(self.update_op_list)
                    self.update_op_list = []
                # get new token if needed
                if self.time_to_new_token < datetime.datetime.now():
                    tmp = utils.get_fflogs_token()
                    self.token_points = True
                    self.fflogs_token = tmp['access_token']
                    self.time_to_new_token = datetime.datetime.now() + datetime.timedelta(seconds=tmp['expires_in'])

                if self.testing:
                    break
        except Exception as e:
            print(traceback.format_exc(), file=sys.stderr)
            await lodestone_session.close()
            await fflogs_session.close()
            await api_session.close()

    async def scrap_character(self, session, character_id=None):
        print(f'scraping {character_id}')
        character_info = {'_id': character_id}  # 'last_checked': datetime.datetime.now()}
        url = f'https://eu.finalfantasyxiv.com/lodestone/character/{character_id}/'
        async with session.get(url) as response:
            if response.status == 200:
                character_page = await response.text()
                character_info = {**character_info, **self.get_character_info(character_page),
                                  'exists': 'S', 'scrapped_lodestone_date': datetime.datetime.now()}
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

        async with self.lock:
            self.update_op_list.append(UpdateOne({"_id": character_info["_id"]}, {"$set": character_info}, upsert=True))

    def get_character_info(self, character_page: str) -> dict:
        tmp = {}
        soup = BeautifulSoup(character_page, 'html.parser')
        tmp['name'] = soup.find('p', class_='frame__chara__name').text
        title_tag = soup.find('p', class_='frame__chara__title')
        if title_tag:
            tmp['title'] = soup.find('p', class_='frame__chara__title').text
        world_dc = soup.find('p', class_='frame__chara__world').text
        tmp['server'], tmp['datacenter'] = world_dc.split(' ')
        tmp['datacenter'] = tmp['datacenter'].replace('[', '').replace(']', '')
        tmp['server'] = self.worlds[tmp['server']]
        tmp['region'] = self.regions[tmp['server']]
        # Not all characters are in a free company
        try:
            a_tag = soup.find('div', class_='character__freecompany__name').find('a')
            # -2 due to the / at the end of the url
            tmp['fc_id'] = a_tag.get('href').split('/')[-2]
        except AttributeError:
            tmp['fc_id'] = None

        # TODO: Normalize classes/jobs names
        tmp['jobs'] = {
            li.img.get('data-tooltip').replace(' (Limited Job)', '').split(' / ')[0]:
                0 if li.text == '-' else int(li.text)
            for li in [x for y in soup.find_all('div', class_='character__level__list') for x in y.find_all('li')]
        }

        return tmp

    async def get_fflogs_info(self, session: aiohttp.ClientSession, fflogs_id=None, character_data=None):
        # todo: rewrite the query since it's very costly, approach to frozen logs
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
            # fflogs api only gives status of the request when the query is badly made or
            # there isn't enough points on the api key
            if 'status' not in response_data.keys():
                response_data = response_data["data"]["characterData"]["character"]
                clean_data = {"_id": response_data["lodestoneID"], "hidden": response_data["hidden"],
                              "fflogs_id": response_data["id"],
                              "raids": {**{key.replace('e_', ''): value
                                           for key, value in response_data.items() if 'e_' in key}}
                              }
                async with self.lock:
                    self.update_op_list.append(UpdateOne({"_id": clean_data["_id"]}, {"$set": clean_data}, upsert=True))
            elif response_data['status'] == 429:
                print('ran out of points')
                async with self.err_lock:
                    self.token_points = False


# a quick test
if __name__ == '__main__':
    scraper = Client(chunk_len=3, testing=False)
    asyncio.run(scraper.main_process())
