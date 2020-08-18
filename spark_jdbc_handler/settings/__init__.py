import os
import json

# 3rd party
from dotenv import load_dotenv, find_dotenv

# load env
load_dotenv(find_dotenv(usecwd=True))

# internal
from .secrets import secrets
from .config import Config