import os
import aiohttp
import asyncio
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import aio_pika

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
TIMEOUT = int(os.getenv("TIMEOUT", 10))

if not RABBITMQ_URL:
    print("Ошибка: переменная окружения RABBITMQ_URL не существует")
    exit(1)

processed_urls = set()


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

                if urlparse(href).netloc == urlparse(url).netloc and href not in processed_urls:
                    links.append(href)
                    processed_urls.add(href)
            return links


async def consume():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    try:
        async with connection:
            channel = await connection.channel()

            queue = await channel.declare_queue('links', durable=True)
            print("Очередь 'links' готова к использованию")

            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=TIMEOUT)

                    if message:
                        async with message.process():
                            url = message.body.decode()
                            print(f"Получена ссылка: {url}")

                            new_links = await fetch_links(url)
                            if new_links:
                                print(f"Найдено {len(new_links)} новых ссылок.")

                                for new_url in new_links:
                                    await channel.default_exchange.publish(
                                        aio_pika.Message(body=new_url.encode()),
                                        routing_key=queue.name
                                    )
                            else:
                                print(f"Ссылки для обработки на странице {url} не найдены.")

                except asyncio.TimeoutError:
                    print(f"Очередь пуста в течение {TIMEOUT} секунд. Продолжаю ожидание...")

    except Exception as e:
        exit(1)


async def main():
    await consume()

if __name__ == "__main__":
    asyncio.run(main())
