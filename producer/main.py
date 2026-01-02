# producer/main.py
import requests
import json
from confluent_kafka import Producer
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from config import settings

# kafka 띄우기
conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'skywatcher-producer'
}

producer = Producer(conf)
topic = 'raw_flight_data'

# 연결 비용 줄이기 위해 session 생성
session = requests.Session()

# 재시도 전략
retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount('https://', HTTPAdapter(max_retries=retries))

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

# 데이터 가져오는 로직 분리 - 429 error (요청 횟수 초과) 발생시 재시도 로직 위함
def fetch_flight_data():
    token = get_token()
    if not token:
        return None

    url = "https://opensky-network.org/api"
    uri = "/states/all"
    
    # Session 헤더에 토큰 업데이트 (이후 요청에 자동 적용)
    session.headers.update({
        "Authorization": f"Bearer {token}"
    })

    while True:
        try:
            # session.get 사용 (연결 재사용)
            response = session.get(url + uri, timeout=10)

            # Case 1: 성공
            if response.status_code == 200:
                return response.json()

            # Case 2: 429 (Too Many Requests) -> 대기 후 continue
            elif response.status_code == 429:
                wait_time = int(response.headers.get('X-Rate-Limit-Retry-After-Seconds', 10))
                print(f"⛔️ [429 Rate Limit] {wait_time}초 대기 중... (제한 풀리는 중)")
                time.sleep(wait_time + 1) # 안전하게 1초 더 대기
                continue

            # Case 3: 그 외 에러
            else:
                print(f"⚠️ API 요청 에러: {response.status_code}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"🌐 네트워크 에러: {e}")
            time.sleep(5)
            return None

# produce 함수
def produce():
    try:
        data = fetch_flight_data()

        if not data:
            return
        
        else:
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