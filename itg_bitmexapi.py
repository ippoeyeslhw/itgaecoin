# coding: utf-8
import datetime
import hashlib
import hmac
import json
import ssl
import urllib
import urllib.parse
import urllib.request
import threading
import queue
import http.client
import bisect
import websocket  # 3rd party lib(https://pypi.python.org/pypi/websocket-client)




class BitmexAPI:
    """
    Bitmex Rest API 클래스
    apikey, secret 을 넣어 생성

    메소드는 Method 와 path 의 조합이며 '_'로 구분됨
    구현되지 않은 API들은 url 참조하여 직접 작성
    모두 블로킹 모드로 작동
    """

    def __init__(self, _apikey, _secret):
        self.apikey = _apikey
        self.secret = _secret
        self.base_url = 'https://www.bitmex.com'
        self.base_pre = '/api/v1'
        self.x_limit = 0
        self.x_remain =0
        self.x_reset = 0
        self.last_status = 200

    def _req(self, method, path, query_dict, body_dict, is_auth):
        rq_header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 ('
                          'KHTML, '
                          'like Gecko) Chrome/60.0.3112.113 Safari/537.36',
            'accept': 'application/json',
            'content-type': 'application/json',
        }
        # query str
        query_str = ''
        if query_dict:
            query_str = '?' + urllib.parse.urlencode(query_dict)

        # body
        body_str = ''
        if body_dict:
            body_str = json.dumps(body_dict)

        # auth
        if is_auth:
            expires = int(datetime.datetime.now().timestamp() * 1000)
            sig = BitmexUtil.gen_signature(
                self.secret, method, self.base_pre + path, query_str, body_str,
                expires)
            rq_header['api-expires'] = str(expires)
            rq_header['api-key'] = self.apikey
            rq_header['api-signature'] = sig

        # req
        url_str = self.base_url + self.base_pre + path + query_str
        if body_str == '':
            req = urllib.request.Request(url_str, method=method,
                                         headers=rq_header)
        else:
            req = urllib.request.Request(url_str, method=method,
                                         headers=rq_header,
                                         data=body_str.encode('utf-8'))
        res = urllib.request.urlopen(req) # type: http.client.HTTPResponse

        self.last_status = res.status

        self.x_limit = res.getheader('x-ratelimit-limit')
        self.x_remain = res.getheader('x-ratelimit-remaining')
        self.x_reset = res.getheader('x-ratelimit-reset')

        return res

    # https://www.bitmex.com/api/explorer/
    # explorer 참고하여 비슷한 형식으로 작성하면 된다.
    def get_position(self, symbol='XBTUSD'):
        res = self._req('GET', '/position',
                        {'filter': json.dumps({"symbol": symbol})}, None, True)
        txt = res.read().decode('utf-8')
        return json.loads(txt)

    # 레버리지 조절
    def post_position_leverage(self, leverage, symbol='XBTUSD'):
        l = {
            'symbol': symbol,
            'leverage': leverage,
        }
        res = self._req('POST', '/position/leverage', None, l, True)
        txt = res.read().decode('utf-8')
        return json.loads(txt)


    def get_user_wallet(self, currency='XBt'):
        res = self._req('GET', '/user/wallet', {'currency': currency}, None,
                        True)
        txt = res.read().decode('utf-8')
        return json.loads(txt)

    # 주문 지정가로만, 음수는 매도
    def post_order(self, order_qty, price, symbol='XBTUSD', is_post_only=False):
        order = {
            'symbol': symbol,
            'orderQty': order_qty,
            'price': price,
        }
        if is_post_only: # 테이킹 주문 방지 (무조건 메이커)
            order['execInst'] = 'ParticipateDoNotInitiate'

        res = self._req('POST', '/order', None, order, True)
        txt = res.read().decode('utf-8')
        return json.loads(txt)

    # 벌크주문
    def post_order_bulk(self, qtys, prices, symbol='XBTUSD',
                        is_post_only=False):
        bulk = []
        for i, (order_qty, price) in enumerate(zip(qtys, prices)):
            order = {
                'symbol': symbol,
                'orderQty': order_qty,
                'price': price,
            }
            if is_post_only: # 테이킹 주문 방지 (무조건 메이커)
                order['execInst'] = 'ParticipateDoNotInitiate'
            bulk.append(order)
        orders = {
            'orders': json.dumps(bulk)
        }
        res = self._req('POST', '/order/bulk', None, orders, True)
        txt = res.read().decode('utf-8')
        return json.loads(txt)


class BitmexWebsocket:
    """
    웹소켓 API구현, 바로 Auth를 실시하므로 apikey, secret 정확히 넣어줄 것,
    실시간 수신데이터는 백그라운드 스레드에서 Queue에 담는다.
    """
    def __init__(self, _apikey, _secret, _get_queue=queue.Queue()):
        self.base_url = 'wss://www.bitmex.com/realtime'
        self.ws = None  # type: websocket.WebSocketApp
        self.apikey = _apikey
        self.secret = _secret
        self.topics = []
        self.msg_q = _get_queue

    def get_message_queue(self):
        return self.msg_q

    def run_with_topics(self, topics):
        self.topics = topics
        self._connect()

    def _connect(self):
        self.ws = websocket.WebSocketApp(
            self.base_url,
            on_message=self._on_message,
            on_open=self._on_open,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        th = threading.Thread(
            target=self._run_forever,
            daemon=True,
        )
        th.start()

    def _run_forever(self):
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_OPTIONAL})

    def send_message(self, op, args):
        self.ws.send(json.dumps({'op': op, 'args': args}))

    def _on_open(self, ws):
        # auth부터 실시한다.
        expires = int(datetime.datetime.now().timestamp() * 1000)
        sig = BitmexUtil.gen_signature(self.secret, 'GET', '/realtime', '', '',
                                       expires)
        self.send_message('authKey', [self.apikey, expires, sig])

        # topics 를 요청
        self.send_message('subscribe', self.topics)

    def _on_message(self, ws, message):
        self.msg_q.put(message)

    def _on_error(self, ws, error):
        print(error)
        self.ws.close()

    def _on_close(self, ws):
        self.ws.close()


class BitmexUtil:
    @classmethod
    def gen_signature(cls, secret, method, path, query_str, body_str, nonce):
        """
        bitmex auth key 생성
        """
        target = method + path + query_str + str(
            nonce) + body_str
        message = bytes(target, 'utf-8')
        signature = hmac.new(bytes(secret, 'utf-8'), message,
                             digestmod=hashlib.sha256).hexdigest()
        return signature


    @classmethod
    def join_topics(cls, topics, symbol='XBTUSD'):
        return [itm + ':'+ symbol if itm != 'wallet' else itm for itm in topics]

    @classmethod
    def normal_topics(cls, symbol='XBTUSD'):
        topics = [
            'position',
            'order',
            'execution',
            'wallet',
            'orderBookL2',
            'trade'
        ]
        return BitmexUtil.join_topics(topics, symbol)

    class WsBalance:
        """
        웹소켓 실시간수신 데이터로 구현되는 잔고
        (주의점: wallet 과 position은 partial 후 update 만 된다는 것을 전제함
         account, currency 가 wallet, position 공통 키이지만 현재 두개는 고정값이므로
         향후 빗맥이 여러 코인을 지원하게 될 때 수정할 것, 현재는 Xbt(비트코인) 하나뿐이다.)
        """
        def __init__(self):
            self.wallet = {}    #  key: account, currency
            self.positions = {} #  key: account, currency, symbol

        def plexing(self, table, action, d):
            if 'wallet' == table:
                d = d['data'][0]  # re - assign
                if 'partial' == action:
                    self.wallet = d
                elif 'update' == action:
                    for k in d.keys():
                        self.wallet[k] = d[k]

            if 'position' == table:
                if 'partial' == action:
                    for t in d['data']:
                        self.positions[t['symbol']] = t
                elif 'update' == action:
                    for t in d['data']:
                        for k in t.keys():
                            self.positions[t['symbol']][k] = t[k]

        def front_wallet_keys(self):
            return [
                'wallet_balance',
                'unrealize_pnl',
                'margin_balance',
                'position_margin',
                'available_balance',
            ]
        def front_wallet(self):
            """
            웹 프론트에 있는 wallet 구현
            """
            if 'amount' not in self.wallet:
                amount = 0
            else:
                amount = self.wallet['amount']

            real_pnl = 0
            unreal_pnl = 0
            pos_margin = 0

            for k in self.positions.keys():
                real_pnl += self.positions[k]['realisedPnl']
                unreal_pnl += self.positions[k]['unrealisedPnl']
                pos_margin += self.positions[k]['maintMargin']

            available = amount + real_pnl + unreal_pnl - pos_margin

            return {
                'wallet_balance': amount + real_pnl,
                'unrealize_pnl': unreal_pnl,
                'margin_balance': amount + real_pnl + unreal_pnl,
                'position_margin': pos_margin,
                'available_balance': available,
            }


    class WsOrderBooks:
        """
        거래상품별 오더북 집합, 실시간수신 데이터를 넣으면 분류하여 오더북생성
        """
        def __init__(self):
            self.books = {}

        def plexing(self, table, action, d):
            if 'orderBookL2' == table:
                for t in d['data']:
                    symbol = t['symbol']
                    if symbol not in self.books:
                        self.books[symbol] = BitmexUtil.WsOrderBook(symbol)

                    if action == 'delete':
                        self.books[symbol].delete(t['id'], t['side'])
                    elif action == 'update':
                        self.books[symbol].update(t['id'], t['side'], t['size'])
                    else:
                        self.books[symbol].insert(t['id'], t['side'], t['price'], t['size'])

        def gen_buys(self, symbol, limit=20):
            """
            지정한 심볼의 1차 ~ limit차 매수호가 제네레이터 (가격,수량,누적)
            """
            if symbol not in self.books:
                return
            accum = 0
            for pr, qty in self.books[symbol].gen_buys(limit):
                accum += qty
                yield pr, qty, accum

        def gen_slls(self, symbol, limit=20):
            """
            지정한 심볼의 1차 ~ limit차 매도호가 제네레이터 (가격,수량,누적)
            """
            if symbol not in self.books:
                return
            accum = 0
            for pr, qty in self.books[symbol].gen_slls(limit):
                accum += qty
                yield pr, qty, accum

    class WsOrderBook:
        """
        오더북 구현, 기본적으로 id 별로 주문을 구분하되, 가격순 정렬을 유지하여
        근접호가 빠른 접근 가능,
        (주의점: 기본 binary search사용, 높은빈도의 slicing, 수신데이터 정합성 체크하지 않음)
        """
        def __init__(self, symbol='XBTUSD'):
            self.symbol = symbol
            self.buy_pr = []  # sorted
            self.buy_id = []
            self.sll_pr = []  # sorted
            self.sll_id = []
            self.buy_orders = {}  # key: iid, {'price':float, 'size':int}
            self.sll_orders = {}  # key: iid, {'price':float, 'size':int}

        def insert(self, iid, side, price, size):
            if side == 'Buy':
                lens = len(self.buy_pr)
                idx = bisect.bisect_left(self.buy_pr, price) # O(logN)
                if lens > 0 and idx < lens:
                    if self.buy_pr[idx] == price:  # already exist
                        print('already exist')
                        return
                self.buy_pr = self.buy_pr[0:idx] + [price] + self.buy_pr[idx:lens]
                self.buy_id = self.buy_id[0:idx] + [iid] + self.buy_id[idx:lens]
                self.buy_orders[iid] = {'price': price, 'size': size}
            else:
                lens = len(self.sll_pr)
                idx = bisect.bisect_left(self.sll_pr, price)
                if lens > 0 and idx < lens:
                    if self.sll_pr[idx] == price:  # already exist
                        print('already exist')
                        return
                self.sll_pr = self.sll_pr[0:idx] + [price] + self.sll_pr[idx:lens]
                self.sll_id = self.sll_id[0:idx] + [iid] + self.sll_id[idx:lens]
                self.sll_orders[iid] = {'price': price, 'size': size}

        def update(self, iid, side, size):
            if side == 'Buy':
                if iid in self.buy_orders:
                    self.buy_orders[iid]['size'] = size
            else:
                if iid in self.sll_orders:
                    self.sll_orders[iid]['size'] = size

        def delete(self, iid, side):
            if side == 'Buy':
                if iid in self.buy_orders:
                    pass
                else:
                    return
                lens = len(self.buy_pr)
                price = self.buy_orders[iid]['price']
                idx = bisect.bisect_left(self.buy_pr, price)
                if lens > 0 and idx < lens:
                    if self.buy_pr[idx] == price:  # exist
                        self.buy_pr = self.buy_pr[0:idx] + self.buy_pr[idx + 1:lens]
                        self.buy_id = self.buy_id[0:idx] + self.buy_id[idx + 1:lens]
                        self.buy_orders.pop(iid, None)
            else:
                if iid in self.sll_orders:
                    pass
                else:
                    return
                lens = len(self.sll_pr)
                price = self.sll_orders[iid]['price']
                idx = bisect.bisect_left(self.sll_pr, price)
                if lens > 0 and idx < lens:
                    if self.sll_pr[idx] == price:  # exist
                        self.sll_pr = self.sll_pr[0:idx] + self.sll_pr[idx + 1:lens]
                        self.sll_id = self.sll_id[0:idx] + self.sll_id[idx + 1:lens]
                        self.sll_orders.pop(iid, None)

        def gen_buys(self, limit=20):
            lens = len(self.buy_pr)
            for i, pr in enumerate(reversed(self.buy_pr)):
                if i < limit:
                    yield pr, self.buy_orders[self.buy_id[lens - i - 1]]['size']
                else:
                    break

        def gen_slls(self, limit=20):
            for i, pr in enumerate(self.sll_pr):
                if i < limit:
                    yield pr, self.sll_orders[self.sll_id[i]]['size']
                else:
                    break



if __name__ == "__main__":
    apikey = 'write_your_apikey'
    secret = 'write_your_secret_key'

    bapi = BitmexAPI(apikey, secret)

    #bapi.post_order(10000, 8630)

    wapi = BitmexWebsocket(apikey, secret)
    msg_q = wapi.get_message_queue()
    wapi.run_with_topics(
        ['position', 'order', 'execution', 'wallet', 'orderBookL2'])

    while True:
        itm = msg_q.get()  # blocking
        a = json.loads(itm)
        print(a)
