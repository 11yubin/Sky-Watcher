# producer/main.py
import requests
import json
from confluent_kafka import Producer
import time
from config import settings

# kafka 띄우기
conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'skywatcher-producer'
}

producer = Producer(conf)
topic = 'raw_flight_data'

## 한국 위경도 - test용!!
# params = {
#     "lamin": 33.00,  # 위도 최소값 (제주도 남단 아래)
#     "lomin": 124.00, # 경도 최소값 (서해안 서쪽)
#     "lamax": 38.61,  # 위도 최대값 (휴전선 북쪽)
#     "lomax": 132.00  # 경도 최대값 (독도 동쪽)
# }

# 콜백 함수
def delivery_report(err, msg):
    if err is not None:
        print(f'❌ 전송 실패: {err}')
    else:
        pass

def get_token():
    client_id = settings.OPENSKY_CLIENT_ID
    client_secret = settings.OPENSKY_CLIENT_SECRET
    token_url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

    token_headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    token = requests.post(token_url, headers=token_headers, data=token_data).json().get('access_token')
    return token

# produce 함수
def produce():
    try:
        token = get_token()
        # 실시간 정보 받아오기
        url = "https://opensky-network.org/api"
        uri = "/states/all"
        headers = {
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(url + uri, headers=headers)
        data = response.json()

        current_time = data.get('time')
        states = data.get('states', [])

        print(f"📡 {current_time} - {len(states)} airplane in the sky")

        for flight in states:
            icao24 = flight[0]
            if not icao24: continue

            message_value = {
                "timestamp": current_time,
                "icao24": icao24,
                "callsign": flight[1].strip(),
                "country": flight[2],
                "longitude": flight[5],
                "latitude": flight[6],
                "altitude": flight[7],
                "velocity": flight[9]
            }

            # kafka 전송
            producer.produce(
                topic, 
                key=icao24.encode('utf-8'), 
                value=json.dumps(message_value).encode('utf-8'), 
                callback=delivery_report
            )
        
        # delivery 확인 (콜백)
        producer.poll(0)

    except Exception as e:
        print(f"⚠️ 에러 발생: {e}")

if __name__=="__main__":
    while True:
        produce()
        time.sleep(15)