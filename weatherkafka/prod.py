import requests
from confluent_kafka import Producer
import json
import socket
import csv

##hava durumu verilerini almak için
class WeatherDataFetcher:
    def __init__(self, api_key, city):
        self.api_key = api_key
        self.city = city
        self.url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={self.api_key}&units=metric"



    def get_weather_data(self):
        response = requests.get(self.url)
        if response.status_code == 200:                #API yanıtı basarılıysa, hava durumu verileri JSON formatında
            data = response.json()     
            weather_data = {
                "city": self.city,                     ##JSON dan alınan veri python dictionary ye donusturuldu 
                "temperature": data['main']['temp'],
                "humidity": data['main']['humidity'],
                "weather": data['weather'][0]['description']  #weather bir liste ilk elemanı almak icin[0]
            }
            return weather_data
        else:
            print("API request failed!")   
            return None



##Kafka ile iletişim kurar. Hava durumu verilerini Kafka'ya gönderir
class KafkaProducerHandler:
    def __init__(self, broker_list,csv_filename="weather_data.csv"):
        self.conf = {
            "bootstrap.servers": ",".join(broker_list),
            "client.id": socket.gethostname()
        }

        self.producer = Producer(self.conf)

        self.csv_filename= csv_filename

        self.write_csv_header()


    ##Geri bildirim icin
    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Message failed to send: {err}")
        else:
            print(f"Message successfully sent: {msg.value().decode('utf-8')}")
            self.write_to_csv(msg.value().decode('utf-8'))



    ##hava durumu verisini Kafka'ya göndermek icin
    def send_weather_data(self, weather_data_json):
        self.producer.produce('weather_topic', value=weather_data_json, callback=self.delivery_report)
        self.producer.flush()
        print(f"Data sent: {weather_data_json}")




    def write_csv_header(self):
        try:
            with open(self.csv_filename,mode='x',newline='',encoding='utf-8') as file:
                writer=csv.writer(file)
                writer.writerow(["city","temperature","humidity","weather"])
        except FileExistsError:
            pass



    def write_to_csv(self,weather_data_json):   ##JSON verisini Python sözlüğüne dönüştürür ve CSV dosyasına yeni bir satır olarak ekler.

        weather_data=json.loads(weather_data_json)
        with open(self.csv_filename,mode='a',newline='',encoding='utf-8') as file:
            writer=csv.writer(file)
            writer.writerow([weather_data["city"], weather_data["temperature"], weather_data["humidity"], weather_data["weather"]])


            
##hava durumu verilerini çeker,Kafka'ya gönderir.
class WeatherDataProducerApp:
    def __init__(self, api_key, city, broker_list):
        self.weather_data_fetcher = WeatherDataFetcher(api_key, city)
        self.kafka_producer_handler = KafkaProducerHandler(broker_list)

        

    def run(self):
        while True:
            input("Press ENTER to send weather data...")

            weather_data = self.weather_data_fetcher.get_weather_data()
            if weather_data:
                weather_data_json = json.dumps(weather_data)
                self.kafka_producer_handler.send_weather_data(weather_data_json)


# main code u calıstırıyor
if __name__ == "__main__":    
    API_KEY = "2a5c0a59f68e64c7ff91489b61e51ca0"  # API key 
    CITY = input("Enter the city name: ") 
    broker_list = ["localhost:29092", "localhost:29093", "localhost:29094"]

    app = WeatherDataProducerApp(API_KEY, CITY, broker_list)
    app.run()
