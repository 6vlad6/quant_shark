import httplib2
import json
import httpx
import asyncio
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime

CREDS_JSON = 'quantshark.json'
sprId = '1tNKx3v6CIUaDJVynp7O18a2hA9_RfIHml5qSRe1iXmQ'

BYBIT_URL = 'https://api.bybit.com/v5/announcements/index?locale=en-US&page={}&limit={}'


async def main():
    """
    Главная функция парсинга. Если файл пустой - добавляет все новости, если файл не пустой - добавляет новые
    :return:
    """
    all_announcements = []  # массив для сохранения всех новостей
    prev_announcements = [] # массив новостей с предыдущей страницы (надо для сбора всех новостей)
    page_num = 1

    def get_service_sacc(creds_json):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']

        creds_service = ServiceAccountCredentials.from_json_keyfile_name(creds_json, scopes).authorize(httplib2.Http())
        return build('sheets', 'v4', http=creds_service)

    resp = get_service_sacc(CREDS_JSON).spreadsheets().values().get(spreadsheetId=sprId, range="news!A:B").execute()
    res = dict(resp)

    if 'values' not in res.keys():  # если файл пустой

        print("Начался парсинг всех новостей")

        async with httpx.AsyncClient(http2=True) as client:
            while True:  # сбор всех новостей
                response = await client.get(BYBIT_URL.format(page_num, 20))
                resp_data = json.loads(response.text)['result']['list']

                announcements = []
                for announcement in resp_data:
                    ann_date = datetime.datetime.fromtimestamp(announcement['dateTimestamp']/1000)  # перевод из мс в с
                    announcements.append([f"{announcement['title']},{announcement['url']},{ann_date}"])

                all_announcements += announcements

                if prev_announcements == announcements:  # проверка, равны ли новости с новой страницы новостям с прошлой
                    break

                prev_announcements = announcements
                page_num += 1

            all_announcements.reverse()  # развернуть массив, чтоб новые новости были внизу таблицы

            get_service_sacc(CREDS_JSON).spreadsheets().values().append(
                spreadsheetId=sprId,
                range="news!A:B",
                body={
                    "majorDimension": "ROWS",
                    "values": all_announcements
                },
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
            ).execute()

            print("Все новости добавлены")

    else:  # если файл не пустой

        print("Начался парсинг новых новостей")

        announcements = resp['values']
        last_announcement = announcements[-1]

        async with httpx.AsyncClient(http2=True) as client:
            response = await client.get(BYBIT_URL.format(1, 10))
            resp_data = json.loads(response.text)['result']['list']

            new_announcements = []

            for announcement in resp_data:
                ann_date = datetime.datetime.fromtimestamp(announcement['dateTimestamp'] / 1000)  # перевод из мс в с
                ann = [f"{announcement['title']},{announcement['url']},{ann_date}"]

                if ann == last_announcement:
                    break

                new_announcements.append(ann)

            if new_announcements:

                new_announcements.reverse()  # развернуть массив, чтоб новые новости были внизу таблицы

                get_service_sacc(CREDS_JSON).spreadsheets().values().append(
                    spreadsheetId=sprId,
                    range="news!A:B",
                    body={
                        "majorDimension": "ROWS",
                        "values": new_announcements
                    },
                    valueInputOption='RAW',
                    insertDataOption='INSERT_ROWS',
                ).execute()

                print("Новые новости добавлены")


async def call_main_periodically():
    while True:
        await main()
        await asyncio.sleep(1)

asyncio.run(call_main_periodically())
