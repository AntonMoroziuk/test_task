import asyncio
import aiohttp
import csv
import re

from bs4 import BeautifulSoup

STORES_FILE = 'stores.csv'
CONTACT_RESOURCES = (
    '/',
    '/pages/about',
    '/pages/about-us',
    '/pages/contact',
    '/pages/contact-us',
)
OUT_FILE = 'stores_out.csv'

def read_urls(file_name: str) -> list[str]:
    urls = []
    with open(file_name, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            urls.append(row['url'])
    return urls

async def get_social(url: str) -> dict:
    social_urls = {'twitter': None, 'facebook': None, 'email': None}
    async with aiohttp.ClientSession() as session:
        for resource in CONTACT_RESOURCES:
            async with session.get(f'https://{url}{resource}') as resp:
                parsed_html = BeautifulSoup(await resp.text(), features='html.parser')
                for key in social_urls:
                    if social_urls[key] is None:
                        link = parsed_html.find(href=re.compile(key))
                        if link is not None:
                            social_urls[key] = link['href']
    return social_urls

async def get_product_links(url: str) -> list[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://{url}/collections/all') as resp:
            parsed_html = BeautifulSoup(await resp.text(), features='html.parser')
            links = parsed_html.find_all(href=re.compile('/products/'))
            links = links[:5] if len(links) > 5 else links
            for i in range(min(5, len(links))):
                links[i] = links[i]['href']
                links[i] = links[i][links[i].index('/products/'):]
                # sometimes link have '#' at the end, so need to remove it
                if links[i][-1] == '#':
                    links[i] = links[i][:-1]
            return links

async def get_products(url:str, product_urls: list[str]) -> dict:
    products = {}
    async with aiohttp.ClientSession() as session:
        for i, product in enumerate(product_urls):
            async with session.get(f'https://{url}{product}.json') as resp:
                try:
                    data = await resp.json()
                except:
                    continue
                if data is None:
                    continue
                data = data['product']
                products[f'title {i}'] = data['title'] if 'title' in data else ''
                if 'images' in data and len(data['images']) > 0 and 'src' in data['images'][0]:
                    products[f'image {i}'] = data['images'][0]['src']
                else:
                    products[f'image {i}'] = ''
    return products

async def get_store_data(url: str) -> dict:
    social = await get_social(url)
    product_urls = await get_product_links(url)
    product_data = await get_products(url, product_urls)
    return {'url': url, **social, **product_data}

def write_data(data: list[dict]):
    columns = max(data, key=len).keys()
    with open(OUT_FILE, 'w') as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

async def main():
    store_urls = read_urls(STORES_FILE)

    data_out = await asyncio.gather(
        *[get_store_data(url) for url in store_urls]
    )
    write_data(data_out)

if __name__ == '__main__':
    asyncio.run(main())