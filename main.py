# -*- coding: utf-8 -*-
"""
Скрипт для сбора информации с сайта https://erzrf.ru
Собирает в файл data*.csv следующую информацию:
Идентификационный номер,
Регион,
Населенный пункт,
Улица,
Номер дома,
Материал наружных стен,
Этажность минимальная,
Этажность максимальная,
Проектная площадь жилых помещений,
Стадия строительства,
Планируемые даты окончания строительства,
Запланированный срок ввода в эксплуатацию,
Дата сбора информации

Для работы установите зависимости из requirements.txt
В файл proxy.txt положите прокси в формате ip:port:login:password
Прокси нужны хорошие и приватные, регион RU, рекомендую https://proxy6.net/?r=54545  (купон для скидки SdSq8wCwJA)
Запустить main.py
"""

import random
import time
import re
import asyncio
import datetime
import csv
import aiohttp
import requests
from loguru import logger


@logger.catch
class House:
    """Конечное здание"""

    def __init__(self, raw_json):
        self.raw_json = raw_json

    @property
    def id_reality(self) -> str:
        """Id"""
        return str(self.raw_json['id'])

    @property
    def region(self) -> str:
        """Регион"""
        return self.raw_json['region']

    @property
    def address(self) -> str:
        """Адрес полный, иногда он примерный и находится в 'adrPrim'"""
        try:
            return self.raw_json['address']['adrPrim']
        except:
            try:
                return self.raw_json['address']
            except:
                return ''

    @property
    def street(self) -> str:
        """Улица в нормальном виде"""
        if self.address == '':
            return ''

        return self.address.split(',')[0]

    @property
    def number_realty(self) -> str:
        """Номер дома"""
        if self.address == '':
            return ''
        return str(self.converter_realty_number(self.address))

    @property
    def build_material(self) -> str:
        """Материал наружных стен"""
        return self.raw_json['buildMaterial']

    @property
    def floor_from(self) -> str:
        """Этажность минимальная"""
        return str(self.raw_json['floorFrom'])

    @property
    def floor_to(self) -> str:
        """Этажность максимальная"""
        return str(self.raw_json['floorTo'])

    @property
    def living_square(self) -> str:
        """Жилая площадь"""
        return str(self.raw_json['livingSquare'])

    @property
    def phase(self) -> str:
        """Стадия"""
        return self.raw_json['phase']

    @property
    def end_plan(self) -> str:
        """Планируемые даты окончания строительства"""
        return self.raw_json['endPlan']

    @property
    def end_to_investor(self) -> str:
        """Запланированный срок ввода в эксплуатацию"""
        try:
            return self.raw_json['endToInvestors'][0]
        except:
            return ''

    @property
    def place(self) -> str:
        """Город"""
        return re.search(r'[А-Я].+', self.raw_json['region'])[0]

    @staticmethod
    def converter_realty_number(address: str) -> str:
        """Вырезаем номер дома и корпус если есть"""
        try:
            cut_address = re.search(r'(?<= д. )\d{1,}', address)[0]
        except:
            cut_address = ''
        try:
            if 'корп.' in address:
                corpus = re.search(r'(?<=корп. ).+', address)[0]
                if cut_address == '':
                    return cut_address
                return f'{cut_address}/{corpus}'
            return cut_address
        except:
            return address

    def __str__(self):
        return f"{self.id_reality},{self.region},{self.place},{self.address}"


@logger.catch
class ApiInstanse:
    """Вся работа с api"""
    header = {
        'user-agent': random.choice(
            ['Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 '
             'YaBrowser/22.7.0 Yowser/2.5 Safari/537.36',
             'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 '
             'Safari/537.36 Edg/104.0.1293.47'
             ])}

    @staticmethod
    def proxy() -> str:
        """Ожидает, что рядом есть proxy.txt"""
        with open('proxy.txt', 'r', encoding='utf-8') as proxy_file:
            proxy_list = proxy_file.readlines()
        assert len(proxy_list) >= 1, 'Добавьте прокси в формате ip:port:login:password в файл proxy.txt'
        ip, port, user, passw = (random.choice(proxy_list)).split(':')
        proxy = f"http://{user}:{passw}@{ip}:{port}"
        return proxy

    @property
    def all_region_dict(self) -> list:
        """Получаем все регионы"""
        url = 'https://erzrf.ru/erz-rest/api/v1/filtered/dictionary?dictionaryType=buildings_regions'
        response = requests.get(url, headers=self.header, timeout=10)
        if response.status_code != 200:
            logger.warning(
                f'https://erzrf.ru/erz-rest/api/v1/filtered/dictionary  [status code {response.status_code}]')
        all_region_dict = response.json()
        del all_region_dict[0]
        return all_region_dict

    def all_gk_in_region(self, region_id: str, region_title: str) -> list:
        """Запрос на все комплексы в регионе"""
        url = f'https://erzrf.ru/erz-rest/api/v1/gk/table?region={region_title}&regionKey={region_id}' \
              f'&costType=1&sortType=cmxrating&min=1&max=10000'
        response = requests.get(url, headers=self.header, timeout=10)
        if response.status_code != 200:
            logger.warning(f'https://erzrf.ru/erz-rest/api/v1/gk/table?region  [status code {response.status_code}]')

        all_reality_id = [i['gkId'] for i in response.json()['list']]
        return all_reality_id

    def all_reality_in_gk(self, region_id: str, region_title: str, gk_id: str) -> dict:
        """Id всех зданий в комплексе"""
        url = f'https://erzrf.ru/erz-rest/api/v1/gk/tabs?gkId={gk_id}&region={region_title}&regionKey={region_id}' \
              f'&costType=1&sortType=qrooms'
        response = requests.get(url, headers=self.header, timeout=10)
        if response.status_code != 200:
            logger.warning(f'https://erzrf.ru/erz-rest/api/v1/gk/tabs?gkId  [status code {response.status_code}]')
        return response.json()

    @staticmethod
    def write_csv(data: list):
        """Записываем результат в data*.csv"""
        now_date = str(datetime.datetime.now().date())
        with open(f"data_{now_date}.csv", "a", encoding="utf-8") as file:
            writer = csv.writer(file)
            for obj in data:
                writer.writerow(
                    (
                        obj.id_reality,
                        obj.region,
                        obj.place,
                        obj.street,
                        obj.number_realty,
                        obj.build_material,
                        obj.floor_from,
                        obj.floor_to,
                        obj.living_square,
                        obj.phase,
                        obj.end_plan,
                        obj.end_to_investor,
                        now_date
                    )
                )

    async def main(self):
        start = time.time()
        now_date = str(datetime.datetime.now().date())

        with open(f"data_{now_date}.csv", "w", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(
                (
                    'Идентификационный номер',
                    'Регион',
                    'Населенный пункт ',
                    'Улица',
                    'Номер дома',
                    'Материал наружных стен',
                    'Этажность минимальная',
                    'Этажность максимальная',
                    'Проектная площадь жилых помещений',
                    'Стадия строительства',
                    'Планируемые даты окончания строительства',
                    'Запланированный срок ввода в эксплуатацию',
                    'Дата сбора информации',
                )
            )
        count = 0
        _data = []
        async with aiohttp.ClientSession() as session:
            for region in self.all_region_dict:
                logger.info(f"Обрабатываю {region['text']}")
                # пишем в файл сразу целый регион для экономии времени
                self.write_csv(_data)
                logger.info("Промежуточный результат успешно сохранен в файл. Продолжаю работу")
                _data = []
                for gk in self.all_gk_in_region(region_id=region['id'], region_title=region['text']):
                    try:
                        all_reality = self.all_reality_in_gk(region_id=region['id'], region_title=region['text'],
                                                             gk_id=gk)
                    except:
                        continue

                    for reality in all_reality:
                        url = f"https://erzrf.ru/erz-rest/api/v1/buildinfo/{reality['id']}?regionKey={region['id']}" \
                              f"&costType=1&sortType=qrooms"
                        async with session.get(url, proxy=self.proxy(), headers=self.header) as resp:
                            data_reality = await resp.json()

                            try:
                                build = House(data_reality)
                            except Exception as error:
                                logger.error(error)
                                continue
                            _data.append(build)
                            count += 1

                            if count % 100 == 0:
                                logger.info(f"Обработано {count} записей")

            if len(_data) > 0:
                self.write_csv(_data)

            logger.success(f"Заняло времени {int(time.time() - start)} секунд. Всего записей: {count} шт")


if __name__ == '__main__':
    asyncio.run(ApiInstanse().main())
