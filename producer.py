import os
import sys
import aiohttp
from bs4 import BeautifulSoup
import pika
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")


async def fetch_links(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Не удалось получить страницу {url}, статус: {response.status}")
                return []
            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            title = soup.title.string if soup.title else "Без названия"
            print(f"Обрабатывается страница: {title} ({url})")

            links = []
            for a_tag in soup.find_all("a", href=True):
                href = urljoin(url, a_tag["href"])
                link_text = a_tag.get_text(strip=True) if a_tag.get_text(strip=True) else "Без названия"

                print(f"Найдена ссылка: {link_text} — {href}")

                if urlparse(href).netloc == urlparse(url).netloc:
                    links.append(href)
            return links


async def main():
    if len(sys.argv) < 2:
        print("Использование: python producer.py <URL>")
        sys.exit(1)

    url = sys.argv[1]

    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        channel.queue_declare(queue="links")
        print(f"Подключено к RabbitMQ. Начинаем обработку ссылок для {url}")
    except Exception as e:
        print(f"Ошибка подключения к RabbitMQ: {e}")
        sys.exit(1)

    links = await fetch_links(url)

    for link in links:
        try:
            print(f"Отправляем ссылку в очередь: {link}")
            channel.basic_publish(
                exchange='',
                routing_key='links',
                body=link
            )
        except Exception as e:
            print(f"Ошибка при отправке сообщения в очередь: {e}")

    connection.close()
    print("Завершено отправление всех ссылок в очередь.")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
    input("")
