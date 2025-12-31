# config.py
# pydantic을 이용한 env파일 관리
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    OPENSKY_CLIENT_ID: str
    OPENSKY_CLIENT_SECRET: str

    class Config:
        env_file = "../.env"
        env_file_encoding = 'utf-8'

settings = Settings()