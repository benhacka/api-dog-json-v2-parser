#!/usr/bin/env python3
import argparse
import datetime
import json
import os
from typing import List, Dict, Union
from pathlib import Path
import asyncio
import logging

import aiofiles
import aiohttp

verify_file_head = '{"meta":{"v":"2.0"'


class DownloadManager:
    def __init__(self, download_limiter):
        self._download_semaphore = asyncio.Semaphore(download_limiter)
        self._download_tuples = []

    @staticmethod
    def _convert_timestamp_to_str(timestamp):
        return datetime.datetime.fromtimestamp(timestamp).strftime(
            '%Y%m%d_%H%M%S')

    def _add_download_tuple(self, root_path, json_name,
                            photo_dict: Dict[str, Union[int, str]]):
        owner_folder = str(photo_dict['owner_id'])
        str_date = self._convert_timestamp_to_str(photo_dict['date'])
        url = photo_dict['photo_url']
        file_name = f'{str_date}_{url.split("/")[-1]}'
        photo_path = os.path.join(root_path, json_name, owner_folder,
                                  file_name)
        self._download_tuples.append((photo_path, url))

    def add_dict(self, file_dict):
        root_path = file_dict['path']
        url_data = file_dict['url_data']
        for file_name_folder, data in url_data.items():
            for photo_dict in data:
                self._add_download_tuple(root_path, file_name_folder,
                                         photo_dict)

    @property
    def _headers(self):
        return {
            'User-Agent':
            ('Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
             '(KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36')
        }

    async def _download_photo(self, file_name, url, *, try_count=3):
        if os.path.isfile(file_name) and os.path.getsize(file_name):
            return 0
        async with self._download_semaphore, aiohttp.ClientSession(
                headers=self._headers) as session:
            session: aiohttp.ClientSession
            try:
                async with session.get(url) as request:
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)
                    async with aiofiles.open(file_name, 'wb') as file:
                        await file.write(await request.read())
                    return 0
            except aiohttp.ClientError:
                if not try_count:
                    return 1
                return await self._download_photo(file_name,
                                                  url,
                                                  try_count=try_count - 1)

    def download_photos(self):
        download_coroutines = [
            self._download_photo(*data) for data in self._download_tuples
        ]
        result = asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*download_coroutines))
        error_count = sum(result)
        if not error_count:
            print(f'All {len(download_coroutines)} photos downloaded')
        else:
            print(f'{error_count}/{len(download_coroutines)} '
                  f'was not downloaded')


class ParserManager:
    def __init__(self, folder_with_json):
        self._folder_with_json = folder_with_json
        self._files = self._filter_folder_files()
        abspath = os.path.abspath(folder_with_json)
        print(f'There are {len(self._files)} json files were caught in '
              f'{abspath}')
        self._parsed_data: Dict[str, Dict[str, List[dict]]] = {
            'path': abspath,
            'url_data':
            {self._convert_file_name_to_key(file): []
             for file in self._files},
        }

    @staticmethod
    def _convert_file_name_to_key(file_name):
        return Path(file_name).stem

    @property
    def is_contain_files(self):
        return bool(self._files)

    @property
    def data_dict(self):
        return self._parsed_data

    @property
    def has_content(self):
        return bool(sum(self._parsed_data.get('url_data', {}).values(), []))

    @staticmethod
    def _verify_file(file):
        if not os.path.isfile(file):
            return None
        try:
            with open(file, mode='r', encoding='utf-8') as fp:
                file_head = fp.read(len(verify_file_head) + 1)
                if verify_file_head in file_head:
                    return file
            return None
        except OSError:
            return None

    def _filter_folder_files(self):
        folder = self._folder_with_json
        if not os.path.isdir(folder):
            return []
        files = [
            self._verify_file(os.path.join(folder, file))
            for file in os.listdir(folder) if file.endswith('.json')
        ]
        return [file for file in files if file]

    def _add_photo_to_parsed_data(self, owner_id, date, photo_url, file_name):
        key = self._convert_file_name_to_key(file_name)
        self._parsed_data['url_data'][key].append({
            'owner_id': owner_id,
            'date': date,
            'photo_url': photo_url
        })

    def _parse_attachments(self, attachment_list: List[dict], file_name):
        if not attachment_list:
            return
        for attachment_dict in attachment_list:
            if attachment_dict.get('type') != 'photo':
                continue
            photo = attachment_dict.get('photo', {})
            if not photo:
                continue
            sizes = photo.get('sizes', [])
            if not sizes:
                continue
            owner_id = photo.get('owner_id', 0)
            date = photo.get('date', 0)
            photo_dict = max(sizes, key=lambda x: x.get('width', 0))
            if not photo_dict or 'url' not in photo_dict:
                continue
            photo_url = photo_dict['url']
            self._add_photo_to_parsed_data(owner_id, date, photo_url,
                                           file_name)

    def _parse_message(self, data_dict: dict, file_name):
        attachments = data_dict.get('attachments', [])
        if attachments:
            self._parse_attachments(attachments, file_name)
        fwd_messages = data_dict.get('fwd_messages', [])
        if not fwd_messages:
            return
        for fwd_message in fwd_messages:
            self._parse_message(fwd_message, file_name)

    def _parse_messages(self, data_dict: dict, file_name):
        data_list = data_dict.get('data', [])
        for data in data_list:
            self._parse_message(data, file_name)

    def parse_files(self):
        if not self.is_contain_files:
            return {}
        for file in self._files:
            with open(file, 'r', encoding='utf-8-sig') as fp:
                self._parse_messages(json.load(fp), file)


def arg_parser():
    pass


def path_grabber():
    pass


def main(parse_folders, download_limiter):
    download_manager = DownloadManager(download_limiter)
    parsers = [ParserManager(folder) for folder in parse_folders]
    for parser in parsers:
        parser.parse_files()
        if not parser.has_content:
            continue
        download_manager.add_dict(parser.data_dict)
    download_manager.download_photos()


if __name__ == '__main__':
    folders = ['.']
    limiter = 50
    main(folders, limiter)
