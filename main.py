import binascii
import os
import time

from dotenv import load_dotenv
from fastapi import Body, FastAPI, Query, Response, status, Request
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.collection import Collection
from prefix_tree import PrefixTree
from base64 import b64decode, b64encode
from typing import Annotated
from bson.objectid import ObjectId

load_dotenv()

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

prefix_tree: PrefixTree
documents: Collection
should_reconnect: bool = False


def decode_path(path: str):

    try:
        raw_path = b64decode(path)
    except binascii.Error:
        return []

    if len(raw_path) % 32 != 0 or len(raw_path) < 32 or len(raw_path) > 2048:
        return []

    return [raw_path[i:i + 32] for i in range(0, len(raw_path), 32)]


def connect():
    global prefix_tree, documents, should_reconnect
    client = MongoClient(os.getenv('MONGODB_CONNECTION_STRING'))
    db = client[os.getenv('MONGODB_DB_NAME')]
    prefix_tree = db[os.getenv('MONGODB_PREFIXTREE_COLLECTION')]
    documents = db[os.getenv('MONGODB_DOCUMENTS_COLLECTION')]
    prefix_tree = PrefixTree(prefix_tree, ObjectId(os.getenv('MONGODB_PREFIXTREE_ROOT_ID')))
    should_reconnect = False


@app.on_event("startup")
async def startup():
    connect()


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    if request.method != 'OPTIONS' and (
            'X-Secret' not in request.headers or request.headers['X-Secret'] != os.getenv('API_TOKEN')):
        response = Response()
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return response
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


@app.get("/uav")
async def search(q: Annotated[str, Query()], response: Response = None):
    global prefix_tree, documents, should_reconnect

    if should_reconnect:
        connect()

    q_list = decode_path(q)

    if len(q_list) == 0:
        response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
        return {'error': 'invalid query data'}

    # prevent from empty or too short search queries
    if len(q_list) == 0 or len(q_list) > 32:
        response.status_code = status.HTTP_403_FORBIDDEN
        return {'error': 'invalid query length'}

    result = prefix_tree.search(prefix_tree.get_dymmyroot(), q_list, max_depth_overhead=30)
    result = list(documents.find({'_id':{'$in': result}}))

    if len(result) == 0:
        response.status_code = status.HTTP_404_NOT_FOUND
        return []

    response_data = []

    for item in result:
        response_data.append({'id': str(item['_id']), 'data': b64encode(item['data'])})

    return response_data


@app.post("/uav")
async def create(path: Annotated[str, Body()], document: Annotated[str, Body()], response: Response = None):
    global prefix_tree, documents, should_reconnect

    path_list = decode_path(path)

    if len(path_list) == 0:
        response.status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
        return {'error': 'invalid path data'}

    inserted_document = documents.insert_one({'data': b64decode(document)})

    try:
        inserted, updated = prefix_tree.insert(path_list, inserted_document.inserted_id)
    except:
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        documents.delete_one({'_id': inserted_document.inserted_id})

    should_reconnect = True

    try:
        documents.database.command({
            "planCacheClear": os.getenv('MONGODB_DOCUMENTS_COLLECTION')
        })

        prefix_tree.get_collection().database.command({
            "planCacheClear": os.getenv('MONGODB_PREFIXTREE_COLLECTION')
        })
    except:
        pass

    return {}
