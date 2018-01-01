"""
This module provides an api to pull data from futbin
"""

import time
import re

import logging
from concurrent.futures import as_completed, Future

from collections import defaultdict
from requests_futures.sessions import FuturesSession
from bs4 import BeautifulSoup

FUTBIN_URL = 'https://www.futbin.com'
FIFA18_URL = f'{FUTBIN_URL}/18'


def get_clubs(start_page=1, end_page=25):
    """
    Get all the available clubs from futbin.
    :param start_page: 
    :param end_page: 
    :return: 
    """

    result = []

    with FutBinSession() as session:

        futures = []
        for i in range(start_page, end_page+1):
            f = ClubsPageFuture(session, page=i)
            futures.append(f)

        for f in _as_completed(futures):
            result.extend(f.result())

    return result


def get_all_players():
    clubs = get_clubs()
    players = get_players_for_clubs(clubs)
    return players


def get_players_for_clubs(clubs):
    """
    return a dict of club -> player links.
    :param clubs: 
    :return: 
    """

    result = defaultdict(list)
    with FutBinSession() as session:
        futures = []
        for club in clubs:
            futures.append(ClubPlayersFuture(session, club))
            logging.debug(f'requested players for {club}')

        futures2 = []
        for f in _as_completed(futures):
            for url in f.result():
                r = PlayerDataFuture(session, url)
                r.club = f.club
                futures2.append(r)
                logging.debug(f'requested url')

        for f in _as_completed(futures2):
            p = f.result()
            result[f.club].append(p)
            logging.debug(f'got {p}')

    return result


def get_player_prices(players, **kwargs):
    """
    return the prices for the provided players
    
    :param players: 
    :param kwargs: 
    :return: 
    """
    logging.debug(f'price request: {kwargs}')

    with FutBinSession() as session:
        futures = []
        for player in players:
            f = PlayerPriceFuture(session, player, **kwargs)
            futures.append(f)
            logging.debug(f'requested prices for {player}')

        for f in _as_completed(futures):
            yield f.player, f.result()
            logging.debug(f'got prices for {f.player}')


class FailedRequestException(Exception):
    pass


class FutBinFuture:
    """
    Provide a wrapper around a Future which allows us to control 
    how specific future results should be parsed.
    """
    def __init__(self, session, url, params=None):
        self.url = url
        self.future = session.get(url, params=params)
        self.future.wrapper = self

    def result(self):
        response = self.future.result()
        if response.status_code != 200:
            raise FailedRequestException(self.url)

        return response


class ParsedFuture(FutBinFuture):
    def result(self):
        response = super(ParsedFuture, self).result()
        html = response.content
        soup = BeautifulSoup(html, 'html.parser')
        return soup


class ClubsPageFuture(ParsedFuture):
    filter = re.compile(r'player_tr_\d+')

    def __init__(self, session, page):
        super(ClubsPageFuture, self).__init__(
            session,
            f'{FIFA18_URL}/clubs',
            params={'page': page})
        self.page = page

    def result(self):
        soup = super(ClubsPageFuture, self).result()
        rows = soup.find_all(class_=self.filter)

        result = []
        for row in rows:
            club = row.find('td').text.strip()
            if club == 'No Results':
                break
            else:
                result.append(club)

        return result


class ClubPlayersFuture(ParsedFuture):
    href_filter = re.compile('player/')

    def __init__(self, session, club):
        super(ClubPlayersFuture, self).__init__(session, f'{FIFA18_URL}/clubs/{club}')
        self.club = club

    def result(self):
        soup = super(ClubPlayersFuture, self).result()
        links = soup.find_all('a', href=self.href_filter)

        for l in links:
            url = l.attrs['href']
            yield FUTBIN_URL + url


class PlayerDataFuture(ParsedFuture):
    def result(self):
        soup = super(PlayerDataFuture, self).result()
        data = soup.find('div', id='page-info')
        return Player.from_url(self.url, data)


class PlayerPriceFuture(FutBinFuture):
    def __init__(self, session, player, type='today', year='18'):
        super(PlayerPriceFuture, self).__init__(
            session,
            url=f'{FUTBIN_URL}/{year}/playerGraph',
            params={
                'type': type,
                'year': year,
                'player': player.price_id,
                '_': int(time.time())
            }
        )
        self.player = player

    def result(self):
        response = super(PlayerPriceFuture, self).result()
        return response.json()


class Player:
    def __init__(self, id, name, url, data):
        self.id = id
        self.name = name
        self.url = url
        self.data = data

    @property
    def price_id(self):
        return self.data['data-player-resource']

    @classmethod
    def from_url(cls, url, *args, **kwargs):
        id, name = url.split('/')[-2:]
        return cls(id, name, url, *args, **kwargs)

    def __repr__(self):
        return f'Player({self.id}, {self.name}, {self.url}, {self.data})'

    def __str__(self):
        return f'{self.name}({self.id})'


class FutBinSession(FuturesSession):
    DEFAULT_WORKERS = 4

    def __init__(self, max_workers=None, *args, **kwargs):
        super(FutBinSession, self).__init__(max_workers=max_workers or self.DEFAULT_WORKERS, *args, **kwargs)


def _as_completed(futures):
    for f in as_completed([f.future for f in futures]):
        yield f.wrapper
