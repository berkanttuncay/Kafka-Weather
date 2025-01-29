from confluent_kafka import Consumer
import json


broker_list = ["localhost:29092", "localhost:29093", "localhost:29094"]
# Kafka consumer ayarları

conf = {
    "bootstrap.servers": ",".join(broker_list),  # Brokerları virgulle ayırarak birlestirir
    "group.id": "weather_consumer",  # Consumer group ID
    "auto.offset.reset": "earliest"  # Mesaj yoksa, bastan baslasın
    
}

consumer = Consumer(conf)    ##Kafka consumer'ı başlatır

def process_weather_data(weather_data):
    # Hava durumu verisini isleme (örneğin, sıcaklık, nem, vb.)
    weather_description = weather_data.get('weather', 'Bilinmiyor')
    temperature = weather_data.get('temperature', 'Bilinmiyor')
    humidity = weather_data.get('humidity', 'Bilinmiyor')
    
    print(f"Weather: {weather_description}")
    print(f"Temperature: {temperature}°C")
    print(f"Humidity: {humidity}%")

# Consumer başlatılıyor ve mesajları bekliyor
if __name__ == '__main__':  #dogrudan calıstırılıyor mu baska dosyadan mı ona bakılır 
    print("Consumer is initialized and is waiting for messages...")

    # Kafka topic'e abone ol
    consumer.subscribe(['weather_topic'])  # Producer'ın kullandığı topic adı

    try:
        while True:
            # Kafka'dan mesaj al
            msg = consumer.poll(timeout=1.0)  # Timeout 1 saniye

            if msg is None:
                continue  # Eğer mesaj yoksa, döngüye devam et
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Kafka'dan gelen ham byte verisini çözümleyerek JSON'a dönüştürme
            
            weather_data = json.loads(msg.value().decode('utf-8'))  # Byte -> String -> JSON (Python dict)
            print(f"Data from Kafka: {weather_data}")
            process_weather_data(weather_data) ##Alınan veriyi, process_weather_data fonksiyonu ile isler ve ekrana yazdırır.
            

    except KeyboardInterrupt:
        print("Consumer is stopped.")
    finally:
        consumer.close()