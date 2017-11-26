# coding: utf-8
import math
import re
import urllib.request

import itgaecoin.itg_orderbooks as orderbooks


# 달러 환율
def usdex_coinone() -> float:
    jsoned = orderbooks.get_url_by_json('https://api.coinone.co.kr/currency/')
    rate = float(jsoned['currency'])
    return rate


def usdex_naver() -> float:
    """
    네이버 USDKRW 환율 최신 고시회차 기준환율 (regex사용, Web Scrap, KEB하나은행)
    :return: usd-krw rate
    """
    url_str = 'http://info.finance.naver.com/marketindex/exchangeDegreeCount' \
              'Quote.nhn?marketindexCd=FX_USDKRW'
    res = urllib.request.urlopen(url_str)
    txt = res.read().decode('euc-kr')  # 인코딩 유의

    pattern = re.compile(r'<tr class="up">\s+<td class="count">([^<]*)</td>\s+'
                         r'<td class="num">([^<]*)</td>')

    # 첫번쨰 매치되는것이 가장 최신의 고시환율
    m = pattern.search(txt)
    if m:
        rate_str = m.group(2)
        rate = float(rate_str.replace(',', ''))
    else:
        raise Exception('not matched, check regex')
    return rate


def check_premium(a_spread, b_spread, a_ex, b_ex) -> dict:
    """
    두개의 스프레드를 비교 프리미엄을 알아낸다.
    :param a_spread: a 거래소 스프레드 (매수1호가,매수1잔량,매도1호가,매도1잔량)
    :param b_spread: b 거래소 스프레드 (매수1호가,매수1잔량,매도1호가,매도1잔량)
    :param a_ex: a 거래소 변환 환율
    :param b_ex: b 거래소 변환 화율
    :return:
    """
    ret = {}

    # long(a) => short(b)
    # a거래소에서 매수 b거래소에서 매도시 단가 및 수익률과 가능수량
    krw_buy = math.floor(a_spread[2] * a_ex)   # sell price 즉각매수호가
    krw_sll = math.floor(b_spread[0] * b_ex)   # buy price  즉각매도호가
    min_qty = min(a_spread[3], b_spread[1])    # 두 잔량중 적은 쪽의 수량
    krw_pft = ((krw_sll - krw_buy) / krw_buy)  # 수익률
    ret['long_short'] = (a_spread[2], b_spread[0], min_qty, krw_pft)

    # short(a) <= long(b)
    # b거래소에서 매수 a거래소에서 매도시 단가 및 수익률과 가능수량
    krw_buy = math.floor(b_spread[2] * b_ex)   # sell price 즉각매수호가
    krw_sll = math.floor(a_spread[0] * a_ex)   # buy price  즉각매도호가
    min_qty = min(b_spread[3], a_spread[1])    # 두 잔량중 적은 쪽의 수량
    krw_pft = ((krw_sll - krw_buy) / krw_buy)  # 수익률
    ret['short_long'] = (b_spread[2], a_spread[0], min_qty, krw_pft)

    return ret


# bottray checking

def check_bottary(*args):
    # (a_b_p, a_b_q, a_a_p, a_a_q, b_b_p, b_b_q, b_a_p, b_a_q)
    # long(a) short(b) :  a_a_p[0 + 2] -> b_b_p[4 + 0]
    # short(a) long(b) :  a_b_p[0 + 0] <- b_a_p[4 + 2]
    ret = {}
    a_usdex = args[8]  # a usdex
    b_usdex = args[9]  # b usdex

    # long - short
    buy = args[0 + 2]
    sll = args[4 + 0]
    kbuy = math.floor(a_usdex * buy)
    ksll = math.floor(b_usdex * sll)
    qty = min(args[0 + 2 + 1], args[4 + 0 + 1])
    pft = round((ksll - kbuy) / kbuy, 4)
    ret['long_short'] = (buy, sll, pft, qty)

    # short - long
    buy = args[4 + 2]
    sll = args[0 + 0]
    kbuy = math.floor(b_usdex * buy)
    ksll = math.floor(a_usdex * sll)
    qty = min(args[4 + 2 + 1], args[0 + 0 + 1])
    pft = round((ksll - kbuy) / kbuy, 4)
    ret['short_long'] = (buy, sll, pft, qty)

    return ret


if __name__ == "__main__":
    print(usdex_naver())
