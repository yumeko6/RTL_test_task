from os import getenv
from datetime import datetime

from dateutil.parser import parse
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient


# Loading env
load_dotenv()

# Define group format types
GROUP_FORMAT_TYPES: dict = {
        "month": "%Y-%m-01T00:00:00",
        "day": "%Y-%m-%dT00:00:00",
        "hour": "%Y-%m-%dT%H:00:00",
    }


async def aggregate_salaries(
        dt_from: str, dt_upto: str, group_type: str
) -> dict:
    """
    Функция для аггрегирования данных в MongoBD на основе вводных данных.
    :param dt_from: Дата начала аггрегации.
    :param dt_upto: Дата окончания аггрегации.
    :param group_type: Тип группировки данных ('month', 'day', 'hour').
    :return: Словарь аггрегированных данных из MongoDB.
    """
    # Convert input dates from ISO format to datetime objects
    dt_from: datetime = parse(dt_from)
    dt_upto: datetime = parse(dt_upto)
    dt_format: str = GROUP_FORMAT_TYPES.get(group_type)

    # Create a MongoDB client and get a reference to the collection
    client = AsyncIOMotorClient(
        f"mongodb://{getenv('HOST')}:{getenv('PORT')}"
    )
    collection = client[getenv("DATABASE")][getenv("COLLECTION")]

    # Create a group id for the $group stage of the pipeline
    pipeline = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {
                "$dateToString": {"format": dt_format, "date": "$dt"}
            },
            "total": {"$sum": "$value"},
        }},
        {"$sort": {"_id": 1}},
    ]

    # Execute aggregation
    cursor = collection.aggregate(pipeline)
    results = await cursor.to_list(length=None)

    dataset = [result["total"] for result in results]
    labels = [result["_id"] for result in results]

    return {"dataset": dataset, "labels": labels}
