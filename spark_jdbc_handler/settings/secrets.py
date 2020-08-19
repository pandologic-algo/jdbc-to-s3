import os
import json


# secrets
secrets = dict(
    DB_USER=os.getenv('DB_USER'),
    DB_PASSWORD=os.getenv('DB_PASSWORD'),
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID'),
    AWS_SECRET_KEY_ID=os.getenv('AWS_SECRET_KEY_ID')
)