import asyncio
import logging
from typing import Tuple, Match, List, Union

import aiohttp
from tqdm import tqdm

from api_dog_parser_v2.constants_and_enum import TITLE_RE


class NameGrabber:
    def __init__(self, name_grabber_limiter):
        self._name_grabber_semaphore = asyncio.Semaphore(name_grabber_limiter)

    @staticmethod
    async def _parse(vk_id: int, session: aiohttp.ClientSession,
                     tqdm_: tqdm) -> Tuple[int, str]:
        url = f'https://m.vk.com/id{vk_id}'
        str_vk_id = str(vk_id)
        async with session.get(url) as request:
            try:
                html = await request.text()
                title: Match = TITLE_RE.search(html)
                if not title:
                    return vk_id, str_vk_id
                str_title = title.group(1).split('|')[0].strip()
                if str_title.lower() in ('ВКонтакте'.lower(), 'VK'.lower()):
                    return vk_id, str_vk_id
                return vk_id, f'{vk_id} ({str_title})'

            except aiohttp.ClientError:
                return vk_id, str_vk_id
            except Exception as e:  # pylint: disable=broad-except
                msg_ = (f'Exception while trying to get name for {vk_id}'
                        f' - {e}')
                logging.exception(msg_)
                return vk_id, str_vk_id
            finally:
                tqdm_.update()

    async def _bulk_crawl(self, id_list: list) -> None:
        async with self._name_grabber_semaphore, aiohttp.ClientSession(
        ) as session:
            logging.info('Getting VK real names')
            tqdm_ = tqdm(total=len(id_list))
            tasks = [
                self._parse(vk_id=int(vk_id), session=session, tqdm_=tqdm_)
                for vk_id in id_list
            ]
            results = await asyncio.gather(*tasks)
        return results

    def bulk_crawl(self, id_list: List[Union[str, int]]):
        result = asyncio.get_event_loop().run_until_complete(
            self._bulk_crawl(id_list))
        return dict(result)
