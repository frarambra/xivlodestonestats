import asyncio
import aiohttp
import requests
import utils
import fflogs_utils
import sys
import traceback

from bs4 import BeautifulSoup
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta

character_index_api = 'http://127.0.0.1:8000/scraping/lodestone/{}'
fflogs_index_api = 'http://127.0.0.1:8000/scraping/fflogs/{}'


class LodestoneScraper:
    def __init__(self, session, batch_size=10, delay=2):
        self.session = session
        self.delay = delay
        self.mongo_client = MongoClient(utils.MONGO_SERVER)
        self.db = self.mongo_client[utils.DATABASE]
        self.raids = self.db[utils.endgame_metadata].find_one({"raids": {"$exists": True}})['raids']
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
        self.batch_size = batch_size
        self.err_list = []
        self.err_lock = asyncio.Lock()
        self.update_op_list = []
        self.lock = asyncio.Lock()

    async def scrap(self):
        try:
            delay_flag = True
            while True:
                batch_size = 0 if len(self.err_list) >= self.batch_size else self.batch_size-len(self.err_list)
                # prepare the tasks
                async with self.session.get(character_index_api.format(batch_size)) as response:
                    data = await response.json()
                    indexes_to_scrap = \
                        data['lodestone_indexes'] if not self.err_list else self.err_list + data['lodestone_indexes']
                    self.err_list = []
                    tasks = [asyncio.create_task(self.get_character(_)) for _ in indexes_to_scrap]
                    for character, error in await asyncio.gather(*tasks):
                        if error and character not in self.err_list:
                            self.err_list.append(character['_id'])
                            delay_flag = True
                        else:
                            update = UpdateOne({"_id": character["_id"]}, {"$set": character}, upsert=True)
                            self.update_op_list.append(update)

                if self.update_op_list:
                    self.db[utils.character_collection].bulk_write(self.update_op_list)
                    self.update_op_list = []

                if delay_flag:
                    await asyncio.sleep(self.delay)
                    delay_flag = False

        except Exception:
            print(traceback.format_exc(), file=sys.stderr)
        finally:
            await self.session.close()

    async def get_character(self, character_id: int = None) -> tuple:
        character_info = {'_id': character_id}
        error = False
        url = f'https://eu.finalfantasyxiv.com/lodestone/character/{character_id}/'
        async with self.session.get(url) as response:
            if response.status == 200:
                character_page = await response.text()
                character_info = {**character_info, **self.get_character_info(character_page),
                                  'exists': True, 'scrapped_lodestone_date': datetime.now()}
            elif response.status == 404:
                character_info['exists'] = False
            # Lodestone returns 429 if there's too many request from a single endpoint
            elif response.status == 429:
                error = True
            else:
                # Even though this is an error it's worth saving the info since it could be interesting
                character_info['error'] = response.status
                character_info['webpage'] = await response.text()
                print(f'There was an error with character {character_id}')

            return character_info, error

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


class FFlogsScraper:
    """This class scraps FFlogs.com then dumps it into a mongodb collection."""
    # TODO: Rewrite queries using gql
    def __init__(self, session, batch_size=10, mode='simple'):
        self.wait_more_points = False
        self.token_points = None
        self.time_til_points = None
        self.token_points_left = None
        self.time_to_new_token = None
        self.fflogs_token = None
        self.session = session
        self.refresh_token()
        self.refresh_points()

        self.batch_size = batch_size
        self.mongo_client = MongoClient(utils.MONGO_SERVER)
        self.db = self.mongo_client[utils.DATABASE]
        self.raids = self.db[utils.endgame_metadata].find_one({"raids": {"$exists": True}})['raids']

        self.err_lock, self.lock = asyncio.Lock(), asyncio.Lock()
        self.mongo_operations = []

        self.mode = mode
        if mode == 'simple':
            self.query = fflogs_utils.fflogs_basic_query
        elif mode == 'current_tier':
            self.query = fflogs_utils.fflogs_current_tier

    def refresh_token(self):
        tmp = fflogs_utils.get_fflogs_token()
        self.fflogs_token = tmp['access_token']
        self.time_to_new_token = datetime.now() + timedelta(seconds=tmp['expires_in'])

    def refresh_points(self):
        res = self.fflogs_query(fflogs_utils.points_info_query)
        res = res['data']['rateLimitData']
        self.token_points = res['limitPerHour']
        self.token_points_left = self.token_points - res['pointsSpentThisHour']
        self.time_til_points = datetime.now() + timedelta(seconds=res['pointsResetIn'])

    async def scrap(self):
        # Should the API tell clients to stop when there's no information
        # to request from fflogs?
        # Check if there's a way to have a clean logic on query and responses execution
        while True:
            if self.time_to_new_token > datetime.now():
                self.refresh_token()

            while self.wait_more_points:
                if self.time_til_points > datetime.now():
                    self.wait_more_points = False
                    self.refresh_points()

            response = requests.get(fflogs_index_api.format(self.batch_size), verify=False)
            data = response.json()
            filters_to_apply = []

            for fflogs_id in data['fflogs_id']:
                filters_to_apply.append(f'id: {fflogs_id}')
            for character in data['character_data']:
                chara_filter = f'name: "{character["name"]}" serverSlug: "{character["server"]}" ' + \
                               f'serverRegion: "{character["region"]}"'
                filters_to_apply.append(chara_filter)

            if self.mode == 'simple':
                tasks = [asyncio.create_task(self.aio_fflogs_query(self.query.format(_)))
                         for _ in filters_to_apply]
                for response in await asyncio.gather(*tasks):
                    if 'status' in response.keys() and response['status'] == 429:
                        self.wait_more_points = True
                    else:
                        response = self.clean_success_response(response)
                        update = UpdateOne({"_id": response["_id"]}, {"$set": response}, upsert=True)
                        self.mongo_operations.append(update)

            elif self.mode == 'current_tier':
                tasks = []
                for character_filter in filters_to_apply:
                    extra_fields = '' if 'id' in character_filter else 'canonicalID hidden'
                    query = self.query.format(character_filter, extra_fields)
                    task = asyncio.create_task(self.aio_fflogs_query(query))
                    tasks.append(task)
                    break
                for response in await asyncio.gather(*tasks):
                    if 'status' in response.keys() and response['status'] == 429:
                        self.wait_more_points = True
                    else:
                        response = self.clean_success_response(response)
                        response = {
                            '_id': response['_id'], 'scrapped_fflogs_date': response['scrapped_fflogs_date'],
                            'difficulty': response['zoneRankings']['difficulty'],
                            'zone': response['zoneRankings']['zone'],
                            'rankings': [
                                {
                                    str(boss['encounter']['id']): boss['encounter']['name'],
                                    'best_percent': boss['rankPercent'],
                                    'median_percent': boss['medianPercent'],
                                    'total_kills': boss['totalKills'],
                                    'best_job': boss['spec']
                                } for boss in response['zoneRankings']['rankings']
                            ]
                        }
                        update = UpdateOne({"_id": response["_id"]}, {"$set": response}, upsert=True)
                        self.mongo_operations.append(update)

            if self.mongo_operations:
                self.db[utils.character_collection].bulk_write(self.mongo_operations)
                self.mongo_operations = []

    @staticmethod
    def clean_success_response(response):
        response = response['data']['characterData']['character']
        response['_id'] = response['lodestoneID']
        del response['lodestoneID']
        if 'canonicalID' in response.keys():
            response['fflogs_id'] = response['canonicalID']
            del response['canonicalID']
        response['scrapped_fflogs_date'] = datetime.now()

        return response

    async def aio_fflogs_query(self, query: str) -> dict:
        headers = {'Content-Type': "application/json", 'Authorization': f'Bearer {self.fflogs_token}'}
        payload = {'query': query}
        async with self.session.post(fflogs_utils.fflogs_vars['api_url'], headers=headers, json=payload) as response:
            response_data = await response.json()
            return response_data

    def fflogs_query(self, query: str) -> dict:
        headers = {'Content-Type': "application/json", 'Authorization': f'Bearer {self.fflogs_token}'}
        payload = {'query': query}
        fflogs_res = requests.post(fflogs_utils.fflogs_vars['api_url'], headers=headers, json=payload)
        return fflogs_res.json()


async def main():
    async with aiohttp.ClientSession() as session:
        # scraper = LodestoneScraper(session=session, batch_size=10, delay=3)
        # scraper = FFlogsScraper(session, batch_size=3, mode='current_tier')
        # await scraper.scrap()
        pass

# a quick test
if __name__ == '__main__':
    asyncio.run(main())
