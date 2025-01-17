import re
import os
import boto3
import httpx
from datetime import datetime
import pandas as pd
from parsel import Selector
import asyncio
from dotenv import load_dotenv
from fake_useragent import UserAgent

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
KEY_SOURCE = os.getenv("KEY_SOURCE")

ua = UserAgent()

keyword = "smartwatch"
page_start = 1
page_stop = 101

urls = [
    f"https://www.ebay.com/sch/i.html?_nkw={keyword}&_sacat=0&_from=R40&_dmd=2&rt=nc&_ipg=240&_pgn={num}"
    for num in range(page_start, page_stop)
]


async def fetch_url(client, url):
    try:
        response = await client.get(url)
        if response.status_code == 200:
            print(f"Fetch {url} succeeded")

            selector = Selector(response.text)
            catalog = selector.xpath('//*/div[@id="mainContent"]//div[@class="srp-river-main clearfix"]//div[@id="srp-river-results"]/ul/li/div/a').getall()

            get_product_link = [Selector(cat).xpath("//*/@href").get() for cat in catalog]
            get_product_name = [Selector(cat).xpath('//div[2]/div[@class="s-item__title"]//text()').get() for cat in catalog]
            get_product_id = [re.findall("/itm/(\d+)", link)[0] for link in get_product_link]

            df = pd.DataFrame(
                {
                    "product_id": get_product_id,
                    "product_name": get_product_name,
                    "product_link": get_product_link,
                }
            )
            return df
        else:
            print(f"Skipping URL {url}: Status code {response.status_code}")
    except httpx.RequestError as e:
        print(f"An error occurred while requesting {url}: {e}")


async def main():
    async with httpx.AsyncClient(
        http2=True,
        headers={
            "User-Agent": f"{ua.random}",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        },
    ) as client:
        tasks = [fetch_url(client, url) for url in urls]
        results = await asyncio.gather(*tasks)
        return results


def load_to_s3(local_object):
    s3 = boto3.resource("s3",
        endpoint_url="https://s3.ap-southeast-1.amazonaws.com",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
    )

    if s3.Bucket(BUCKET_NAME) not in s3.buckets.all():
        s3.create_bucket(Bucket=BUCKET_NAME, CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-1'})
    
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    object_key = KEY_SOURCE+f"{keyword}-{timestamp}.csv"
    try:
        s3.meta.client.upload_file(
            local_object,
            BUCKET_NAME,
            object_key
        )
        print(f"Data successfully uploaded to s3://{BUCKET_NAME}/{object_key}")
    except Exception as error:
        print(f"Error uploading data to S3: {error}")


if __name__ == "__main__":
    list_df = asyncio.run(main())

    df = pd.concat(list_df)
    local_object = f'data/{keyword}_catalog_raw.csv'
    df.to_csv(local_object, index=False)
    load_to_s3(local_object)