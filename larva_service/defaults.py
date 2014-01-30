DEBUG = False
LOG_FILE = False

USE_S3 = False
S3_BUCKET = "undefined"

TESTING = False

import os
import urlparse

WEB_PASSWORD = os.environ.get("WEB_PASSWORD")

SECRET_KEY = os.environ.get("SECRET_KEY")

# Database
MONGO_URI = os.environ.get('MONGO_URI')
url = urlparse.urlparse(MONGO_URI)
MONGODB_HOST = url.hostname
MONGODB_PORT = url.port
MONGODB_USERNAME = url.username
MONGODB_PASSWORD = url.password
MONGODB_DATABASE = url.path[1:]

# Redis
REDIS_URI = os.environ.get('REDIS_URI')
url = urlparse.urlparse(REDIS_URI)
REDIS_HOST = url.hostname
REDIS_PORT = url.port
REDIS_USERNAME = url.username
REDIS_PASSWORD = url.password
REDIS_DB = url.path[1:]
