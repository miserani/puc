import asyncio
import argparse
import os
import time
from pathlib import Path
import random
from typing import Dict, AsyncGenerator, Union
import logging.config

import asyncpg
from faker import Faker
from faker.providers import credit_card, date_time, geo, person, misc
import geopandas as gpd
from shapely.geometry import Point

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

ip_address = os.environ.get("IP")
POSTGRES_URI = os.environ.get(
    "POSTGRES_URI", f"postgresql://postgres:postgres@{ip_address}:5432"
)
POSTGRES_TABLE_NAME = os.environ.get("POSTGRES_TABLE_NAME", "public.card_events")

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", f"PLAINTEXT://{ip_address}:9092")
KAFKA_TOPIC_NAME = os.environ.get("KAFKA_TOPIC_NAME", "com.miserani.credit_events")

fake = Faker("pt_BR")
fake.add_provider(credit_card)
fake.add_provider(date_time)
fake.add_provider(geo)
fake.add_provider(person)
fake.add_provider(misc)

world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))
brazil = world[world["name"] == "Brazil"].geometry.iloc[0]

inserted_events = 0
start_events = time.time()

kafka_producer = None


async def get_kafka_producer():
    from aiokafka import AIOKafkaProducer

    global kafka_producer
    if kafka_producer is None:
        kafka_producer = AIOKafkaProducer(bootstrap_servers="localhost")
        await kafka_producer.start()
    return kafka_producer


async def close_kafka_producer():
    global kafka_producer
    if kafka_producer is not None:
        kafka_producer = None


def generate_cpf_pool(size=100):
    repetitions = random.randint(5, 10)
    if size < repetitions:
        size = repetitions
    unique_cpfs = [
        fake.random_number(digits=11, fix_len=True) for _ in range(size // repetitions)
    ]

    cpfs = unique_cpfs.copy()
    for _ in range(size - len(unique_cpfs)):
        cpfs.append(unique_cpfs[random.randint(0, len(unique_cpfs) - 1)])

    random.shuffle(cpfs)
    return cpfs


def generate_lat_long_within_brazil():
    while True:
        lat_limits = (-33.0, 5.0)  # Latitude Sul e Norte
        long_limits = (-74.0, -35.0)  # Longitude Oeste e Leste
        lat = random.uniform(*lat_limits)
        long = random.uniform(*long_limits)
        point = Point(long, lat)
        if brazil.contains(point):
            return lat, long


async def generate_records(num_records: int) -> AsyncGenerator[Dict, None]:
    cpf_pool_ = generate_cpf_pool(size=num_records)
    for _ in range(num_records):
        datetime_ = fake.date_time_between(start_date="-180d", end_date="now").replace(
            microsecond=0
        )
        lat, long = generate_lat_long_within_brazil()
        record = {
            "user_id": int(cpf_pool_[_]),
            "cc_num": int(fake.credit_card_number(card_type="mastercard")),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "event_datetime": datetime_,
            "event_unix_time": fake.unix_time(),
            "category": fake.random_element(
                elements=(
                    "grocery",
                    "entertainment",
                    "utility",
                    "rent",
                    "gas",
                    "food",
                    "travel",
                    "clothing",
                    "misc",
                    "shopping",
                )
            ),
            "merchant": fake.company(),
            "value": fake.random_number(digits=4, fix_len=True) / 100,
            "lat": str(lat),
            "long": str(long),
        }
        yield record


async def insert_data_postgres(record: Dict, pool) -> None:
    logger.debug(f"Inserting data into PostgreSQL...\n{record}")
    async with pool.acquire() as conn:
        try:
            global inserted_events
            await conn.execute(
                f"""
                INSERT INTO {POSTGRES_TABLE_NAME} (
                user_id, cc_num, first_name, last_name, event_datetime, event_unix_time, category, merchant, value, lon, lat,
                "location")
                VALUES ({record["user_id"]}, {record["cc_num"]}, '{record["first_name"]}', '{record["last_name"]}', 
                        '{record["event_datetime"].strftime("%Y-%m-%d %H:%M:%S")}', {record["event_unix_time"]}, 
                        '{record["category"]}', '{record["merchant"]}', {record["value"]}, {record['long']}, {record['lat']},
                        'POINT({record['long']} {record['lat']})')"""
            )
            inserted_events += 1
        except Exception as e:
            logger.error(f"Error inserting data: {e}")


async def publish_to_kafka(record: Dict) -> None:
    global inserted_events
    try:
        producer = await get_kafka_producer()

        def convert_datetime_to_str(records):
            import datetime

            for key, value in records.items():
                if isinstance(value, datetime.datetime):
                    records[key] = value.strftime("%Y-%m-%d %H:%M:%S")
            return records

        await producer.send_and_wait(
            KAFKA_TOPIC_NAME,
            key=str(record["user_id"]).encode("utf-8"),
            value=str(convert_datetime_to_str(record)).encode("utf-8"),
        )
        inserted_events += 1
    except Exception as e:
        logger.error(f"Error inserting data: {e}")


async def async_task(num_records, db_type):
    if db_type == "postgres":
        async with asyncpg.create_pool(POSTGRES_URI) as pool:
            async for record in generate_records(num_records):
                await insert_data_postgres(record, pool)
    elif db_type == "kafka":
        async for record in generate_records(num_records):
            await publish_to_kafka(record)


async def run_tasks(args):
    logger.info("Starting data generation...")
    tasks = []
    for _ in range(args.tasks):
        task = asyncio.create_task(async_task(args.records, args.db), name=f"Task {_}")
        tasks.append(task)
    await asyncio.gather(*tasks)
    logger.info("Data generation completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Insert fake data into PostgreSQL and publish to Kafka"
    )
    parser.add_argument(
        "--db",
        choices=["postgres", "kafka"],
        required=True,
        help="Events create type (postgres or kafka)",
    )
    parser.add_argument("--tasks", type=int, default=1, help="Number of tasks")
    parser.add_argument("--records", type=int, default=1000, help="Records per task")
    args = parser.parse_args()

    try:
        asyncio.run(run_tasks(args))

        print(f"Total de Eventos Inseridos no PostgreSQL: {inserted_events}")
        print(
            f"Tempo de Inserção de Eventos no PostgreSQL: {time.time() - start_events}"
        )

        total_insertion_time = time.time() - start_events
        event_insertion_rate = inserted_events / total_insertion_time
        print(
            f"Taxa de Inserção de Eventos no PostgreSQL: {event_insertion_rate} eventos/segundo"
        )
    except Union[KeyboardInterrupt, SystemExit, Exception] as e:
        logger.error(f"Error running tasks: {e}")
    finally:
        asyncio.run(close_kafka_producer())
