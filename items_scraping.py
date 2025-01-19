from datetime import datetime
import aiohttp
import requests
import time
import re
import httpx
import pandas as pd
from parsel import Selector
import asyncio
import os
import io
import boto3
import json
from dotenv import load_dotenv
from fake_useragent import UserAgent

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
KEY_SOURCE = os.getenv("KEY_SOURCE")
KEY_DEST = os.getenv("KEY_DEST")

keyword = 'smartwatch'
ua = UserAgent()

def extract_from_s3():
    s3 = boto3.resource('s3',
        endpoint_url="https://s3.ap-southeast-1.amazonaws.com",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,                            
    )

    objects = []
    for object in s3.Bucket(BUCKET_NAME).objects.all():
        if re.search(KEY_SOURCE, object.key):
            objects.append(object.key)

    df = pd.DataFrame()
    for object in objects:
        s3_object = s3.meta.client.get_object(Bucket=BUCKET_NAME, Key=object)
        object_df = pd.read_csv(io.StringIO(s3_object['Body'].read().decode('utf-8')))
        df = pd.concat([df, object_df])

    return df

async def fetch_url(client, val):
    try:
        url = val['product_link']
        category = val['product_category']
        id = val['product_id']
        response = await client.get(url)
        if response.status_code == 200:
            print(f'Fetch {id} succeeded')
            try:
                selector = Selector(response.text)
                product_title = selector.xpath('//*/h1/span/text()').get()
                product_specifics = selector.xpath('//*/div[@class="tabs__content"]//div[@data-testid="x-about-this-item"]/div/div[2]//span/text()').getall()
                product_price = selector.xpath('//*/div[@data-testid="x-price-section"]//div[@class="x-price-primary"]/span/text()').get()
                product_desc = selector.xpath('//*/div[@class="x-item-description-child"]//text()').getall()
                product_feedback = selector.xpath('//*/div[@class="fdbk-detail-list"]//ul/li//div[@class="fdbk-container__details__comment"]/span/text()').getall()
                product_image = selector.xpath('//*/div[@data-testid="x-photos-min-view"]/div/div[4]/div/img/@ data-zoom-src').getall()

                seller = selector.xpath('//*/div[@data-testid="x-store-information"]/div[@class="x-store-information__header"]/div/h2//text()').get()
                try:
                    seller_id = json.loads(selector.xpath('//*/div[@data-testid="x-store-information"]/div[@class="x-store-information__header"]/div/h2/a/@data-vi-tracking').get())['operationId']
                except Exception:
                    seller_id = None
                seller_url = selector.xpath('//*/div[@data-testid="x-store-information"]/div[@class="x-store-information__header"]/a/@href').get()
                seller_rating = selector.xpath('//*/div[@data-testid="x-store-information"]/div[@class="x-store-information__header"]/div/h4/span[1]/text()').get()

                temp_dict = dict()
                temp_dict = {
                    'title' : product_title,
                    'item_id' : id,
                    'item_url' : url,
                    'item_specifics' : product_specifics,
                    'category' : category,
                    'retail_price' : product_price,
                    'description' : product_desc,
                    'feedback' : product_feedback,
                    'image' : product_image,
                    'seller' : seller,
                    'seller_id' : seller_id,
                    'seller_url' : seller_url,
                    'seller_rating' : seller_rating
                }

                return json.dumps(temp_dict)
            except Exception as e:
                print(f'Error while extracting {id} : {e}')
                return None 
        else:
            print(f'Cannot fetch {id}')
    except Exception as e:
        print(f"An error occurred while requesting {id}: {e}")
        pass

async def main():
    async with httpx.AsyncClient(
        http2=True,
        # headers={
        #     "User-Agent": f"{ua.random}",
        #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        # },
    ) as client:
        df = extract_from_s3()
        tasks = [fetch_url(client, val) for key, val in df.iterrows()]
        result = await asyncio.gather(*tasks)
        return result

def load_to_s3(local_object):
    s3 = boto3.resource("s3",
        endpoint_url="https://s3.ap-southeast-1.amazonaws.com",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

    if s3.Bucket(BUCKET_NAME) not in s3.buckets.all():
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-1'})
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
    object_key = KEY_DEST+f"{timestamp}.json"
    try:
        s3.meta.client.upload_file(
            local_object,
            BUCKET_NAME,
            object_key
        )
        print(f"Data successfully uploaded to s3://{BUCKET_NAME}/{object_key}")
    except Exception as error:
        print(f"Error uploading data to S3: {error}")

if __name__ == '__main__':
    start_time = time.time()
    list_json = asyncio.run(main())

    local_object = f'data/{keyword}_raw.json'
    with open(local_object, 'w', encoding='utf-8') as f:
        json.dump(list_json, f, indent=4, ensure_ascii=False)
    
    load_to_s3(local_object)
    print(f"Total time taken: {time.time() - start_time} seconds")

