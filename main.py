import asyncio
import aiohttp
from client import Client
from server import Server


# https://docs.informatica.com/integration-cloud/cloud-api-manager/current-version/api-manager-guide/authentication-and-authorization/oauth-2-0-authentication-and-authorization/python-3-example--invoke-a-managed-api-with-oauth-2-0-authentica.html
server = Server()
client = Client(chunk_len=5)
client_2 = Client(chunk_len=5)


async def delay_wrapper():
    # Done to avoid race conditions in the same computer as
    # the server will start at the same time as the client pretty much
    await asyncio.sleep(3)
    await client.lodestone_process()


async def delay_wrapper_2():
    # Done to avoid race conditions in the same computer as
    # the server will start at the same time as the client pretty much
    await asyncio.sleep(3)
    await client_2.lodestone_process()

# https://stackoverflow.com/questions/65249795/asyncios-await-reader-read-is-waiting-forever
#loop = asyncio.new_event_loop()
# loop.run_until_complete(asyncio.wait([server.start_server(), delay_wrapper(), delay_wrapper_2()]))


import requests
import json
import logging

logging.captureWarnings(True)

test_api_url = "https://apigw-pod1.dm-us.informaticacloud.com/t/apim.usw1.com/get_employee_details"


def get_fflogs_token():
    with open('./config/client_params.json') as f_read:
        fflogs_vars = json.load(f_read)

    token_res = requests.post(fflogs_vars['token_url'], data={'grant_type': 'client_credentials'},
                              verify=False, allow_redirects=False,
                              auth=(fflogs_vars['client_id'], fflogs_vars['client_secret']))
    return token_res.json()


async def fflogs_data():
    async with aiohttp.ClientSession() as session:
        async with session.get('http://python.org') as response:
