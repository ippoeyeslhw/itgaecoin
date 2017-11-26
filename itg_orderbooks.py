# coding: utf-8
import json
# get request
import time
import urllib.request
from queue import Queue
from threading import Thread
from typing import Generator

REQ_HEADER = {
    'User-Agent': 'ozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
}


def get_url_by_json(url_str) -> dict:
    """
    HTTP REQUEST(GET) and convert json
    :param url_str:  url address
    """
    req = urllib.request.Request(url_str, headers=REQ_HEADER)
    res = urllib.request.urlopen(req)
    txt = res.read().decode('utf-8')
    return json.loads(txt)


# orderbook methods

def book_coinone(currency) -> dict:
    """
    coinone's order book
    :param currency:
    :type currency: str
    """
    url_str = 'https://api.coinone.co.kr/orderbook/?currency=%s&format=json' \
              % (currency.lower())
    return get_url_by_json(url_str)


def book_bittrex(currency, currency_base='usdt') -> dict:
    """
    bittrex's order book
    :param currency:
    :type currency: str
    :param currency_base:
    :return:
    """
    pair = '%s-%s' % (currency_base.upper(), currency.upper())
    url_str = 'https://bittrex.com/api/v1.1/public/getorderbook?market=%s' \
              '&type=both' % pair
    return get_url_by_json(url_str)


# highest buy , lowest sell  spread

def spread_coinone(currency) -> tuple:
    """
    coinone's spreads
    :param currency:
    :type currency: str
    :return:  tupe(buy_price, buy_quantity, sell_price, sell_quantity)
    """
    book = book_coinone(currency)
    return (
        float(book['bid'][0]['price']),
        float(book['bid'][0]['qty']),
        float(book['ask'][0]['price']),
        float(book['ask'][0]['qty']),
    )


def spread_bittrex(currency, currency_base='usdt') -> tuple:
    """
    bittrex's spreads
    :param currency_base:
    :param currency:
    :type currency: str
    :return:  tupe(buy_price, buy_quantity, sell_price, sell_quantity)
    """
    book = book_bittrex(currency, currency_base)
    return (
        float(book['result']['buy'][0]['Rate']),
        float(book['result']['buy'][0]['Quantity']),
        float(book['result']['sell'][0]['Rate']),
        float(book['result']['sell'][0]['Quantity']),
    )


# request spread pair concurrently
# [main thread call] ->  waiting           --------> ->[user logic]--->[recall]
# [worker1]         |-> [coinone]HTTP(GET) -------->|
# [worker2]         |-> [bittrex]HTTP(GET) -->      |
def run_worker(task_q, result):
    """
    워커 스레드에 의해 수행되는 함수
    :param task_q:
    :type task_q: Queue
    :param result:
    :return:
    """
    while True:
        key, func, args = task_q.get()
        try:
            r = func(*args)
            if r:
                result[key] = r
        except Exception as e:
            print(e)
        task_q.task_done()


def iter_spread_with_threadpool(spread_list) -> Generator[dict, None, None]:
    """
    A generator that requests a spread using thread pool.
    스레드풀을 이용하여 스프레드를 요청하는 제네레이터
    ex) spread_list = [('coinone', 'eth'), ('bittrex','eth','btc'),...]
    :param spread_list:
    :type spread_list: list[tuple]
    :return: generator
    """
    # make task queue
    length_of_list = len(spread_list)
    task_q = Queue(length_of_list)
    result = {}  # type: dict

    # func map
    func_map = {
        'coinone': spread_coinone,
        'bittrex': spread_bittrex,
    }

    # start thread pool
    for _ in range(length_of_list):
        thread = Thread(target=run_worker, args=(task_q, result), daemon=True)
        thread.start()

    while True:
        # start
        start = time.time()
        # add tasks
        for spread in spread_list:
            key = '_'.join(spread)
            result[key] = None
            func = func_map[spread[0]]
            args = spread[1:]
            task_q.put((key, func, args))
        # wait completion
        task_q.join()
        # end
        elasped = time.time() - start
        result['elapsed'] = '%s' % elasped

        # be careful request rate limit
        # coinone : 90 requests per minute
        # bittrex: ??
        yield result


if __name__ == "__main__":

    import itgaecoin.itg_bottary as bottary

    spreads = [
        ('coinone', 'eth'),
        ('coinone', 'btc'),
        ('bittrex', 'eth')
    ]

    for ret in iter_spread_with_threadpool(spreads):
        usdex = bottary.usdex_naver()
        print(usdex)

        bot = bottary.check_premium(
            ret['coinone_eth'],
            ret['bittrex_eth'],
            1,
            usdex
        )
        print(bot)
        time.sleep(5)  # check rate limit
