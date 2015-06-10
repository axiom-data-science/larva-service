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



## Sample Runs

#### Super quick
```
{
    "name" : "Sample interp run",
    "behavior": "https://larvamap.s3.amazonaws.com/resources/501c40e740a83e0006000000.json",
    "duration": 10,
    "email": "user@example.com",
    "geometry": "POINT (-147 60.75)",
    "horiz_chunk": 2,
    "horiz_dispersion": 0.01,
    "hydro_path": "/data/lm/datasets/pws/*.nc",
    "particles": 10,
    "release_depth": -2,
    "start": "2014-01-01T01:00:00",
    "time_chunk": 24,
    "timestep": 3600,
    "vert_dispersion": 0.01
}
```

#### Longer
```
{
    "name" : "Sample interp run",
    "behavior": "https://larvamap.s3.amazonaws.com/resources/501c40e740a83e0006000000.json",
    "duration": 20,
    "email": "user@example.com",
    "geometry": "POINT (-147 60.75)",
    "horiz_chunk": 2,
    "horiz_dispersion": 0.01,
    "hydro_path": "/data/lm/datasets/pws/*.nc",
    "particles": 10,
    "release_depth": -2,
    "start": "2014-01-01T01:00:00",
    "time_chunk": 24,
    "timestep": 3600,
    "vert_dispersion": 0.01
}
```

#### Over DAP
```
{
    "name" : "Sample interp run",
    "behavior": "https://larvamap.s3.amazonaws.com/resources/501c40e740a83e0006000000.json",
    "duration": 1,
    "email": "user@example.com",
    "geometry": "POINT (-147 60.75)",
    "horiz_chunk": 2,
    "horiz_dispersion": 0.01,
    "hydro_path": "http://thredds.axiomalaska.com/thredds/dodsC/PWS_DAS.nc",
    "particles": 1,
    "release_depth": -2,
    "start": "2014-01-01T01:00:00",
    "time_chunk": 24,
    "timestep": 3600,
    "vert_dispersion": 0.01
}
```

#### Production
```
{
    "name" : "PDX Test - 10 day run",
    "duration": 10,
    "email": "kyle@axiomdatascience.com",
    "geometry": "POINT (-147 60.75)",
    "horiz_dispersion": 0.01,
    "hydro_path": "/mnt/gluster/data/netCDF/pws_das/processed/pws_das_*.nc",
    "particles": 100,
    "release_depth": -2,
    "start": "2014-06-01T01:00:00",
    "timestep": 3600,
    "vert_dispersion": 0.01
}
```
