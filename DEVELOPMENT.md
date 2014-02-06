## Development Environment

Assumes you have:
* python >= 2.7.2
* MongoDB >= 1.8.2 (running)
* foreman (ruby gem)
* redis (running)

### Install the requirements
    $ pip install -r requirements.txt

### Create an .env file with the following contents
    APPLICATION_SETTINGS=development.py
    SECRET_KEY='yoursupersercetkey'

    MONGO_URI="mongodb://localhost:27017/larvaservice_development"
    REDIS_URI="redis://localhost:6379/0"

    WEB_PASSWORD=yourdesiredwebpass

    # Should we upload results to S3?  If so, all four are required
    USE_S3=True
    S3_BUCKET=yourbucket
    AWS_ACCESS_KEY_ID=your_access_key
    AWS_SECRET_ACCESS_KEY=your_secret_key

    # If not using S3, provide root URL to download results
    USE_S3=False
    NON_S3_OUTPUT_URL="http://localhost/lmfiles/"

    BATHY_PATH="PATH_TO_BATHY" (see README.md for download link)
    OUTPUT_PATH="/data/larvamap/output" (optional, defaults to "./output")
    CACHE_PATH="/tmp/cache" (optional, defaults to "./cache")
    SHORE_PATH="/data/lm/shore/global/10m_land.shp" (optional, defaults to global 10m polygons)

### Edit testing.py is you will be running the tests

### Start the local server
    $ foreman start
