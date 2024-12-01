import os
import sys
import aiohttp
import asyncio
from aio_pika import connect_robust, Message
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")

if not RABBITMQ_URL:
    print("Ошибка: переменная окружения RABBITMQ_URL не существует")
    sys.exit(1)

processed_urls = set()


async def fetch_links(url):
    try:
        print(f"Начинаем обрабатывать URL: {url}")
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
    except Exception as e:
        print(f"Ошибка при обработке URL {url}: {e}")
        return []


async def send_to_queue(links):
    try:
        print("Отправляются ссылки в очередь RabbitMQ...")
        connection = await connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue("links", durable=True)
            for link in links:
                message = Message(link.encode(), delivery_mode=2)
                await channel.default_exchange.publish(message, routing_key="links")
                print(f"Отправлена ссылка: {link}")
    except Exception as e:
        print(f"Ошибка при отправке сообщения в очередь RabbitMQ: {e}")


async def main():
    if len(sys.argv) < 2:
        print("Использование: python producer.py <URL>")
        sys.exit(1)

    url = sys.argv[1]

    try:
        links = await fetch_links(url)
        if not links:
            print(f"Нет ссылок для обработки на {url}")
            return

        await send_to_queue(links)
    except Exception as e:
        print(f"Ошибка в main(): {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Ошибка при запуске асинхронной задачи: {e}")
        sys.exit(1)

    input("Конец")

