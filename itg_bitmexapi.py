# coding: utf-8
import datetime
import hashlib
import hmac
import json
import logging
import logging.handlers
import ssl
import urllib
import urllib.parse
import urllib.request
import threading
import queue
import http.client

import websocket  # 3rd party lib(https://pypi.python.org/pypi/websocket-client)


# 간단한 ORDID 생성
class SimpleOrdIDFactory:
    @staticmethod
    def get_ordid(pre, symbol='simple', price=0):
        prefix = '%s,%s,%s,' % (pre, symbol, str(price))
        postfix = int(datetime.datetime.now().timestamp() * 1000)
        return prefix + str(postfix)


class BitmexAPI:
    """
    Bitmex Rest API 클래스
    apikey, secret 을 넣어 생성
    ordid_factory는 get_ordid 메서드가 구현된 것으로 커스텀 주문ID를 생성하는 객체

    메소드는 Method 와 path 의 조합이며 '_'로 구분됨
    구현되지 않은 API들은 url 참조하여 직접 작성
    모두 블로킹 모드로 작동
    """

    def __init__(self, _apikey, _secret, _ordid_factory=SimpleOrdIDFactory()):
        self.apikey = _apikey
        self.secret = _secret
        self.base_url = 'https://www.bitmex.com'
        self.base_pre = '/api/v1'
        self.ordid_factory = _ordid_factory
        self.x_limit = 0
        self.x_remain =0
        self.x_reset = 0

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
            'clOrdID': self.ordid_factory.get_ordid(' ', symbol, price),
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
                'clOrdID': self.ordid_factory.get_ordid(str(i), symbol, price),
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
        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

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
    def front_wallet(cls, position_json, user_wallet_json):
        """
        웹 프론트에 있는 wallet 구현
        """
        amount = user_wallet_json['amount']
        real_pnl = position_json['realisedPnl']
        unreal_pnl = position_json['unrealisedPnl']
        pos_margin = position_json['maintMargin']
        available = amount + real_pnl + unreal_pnl - pos_margin

        return {
            'wallet_balance': amount + real_pnl,
            'unrealize_pnl': unreal_pnl,
            'margin_balance': amount + real_pnl + unreal_pnl,
            'position_margin': pos_margin,
            'available_balance': available,
        }

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
            'orderBookL2'
        ]
        return BitmexUtil.join_topics(topics, symbol)

    class WsRecentData:
        """
        웹소켓 데이터 수신시 테이블별 데이터 관리
        """
        def __init__(self):
            self.data = {}
            self.key = {}
            # 기본 로깅 세팅
            self.logger = logging.getLogger(self.__class__.__name__)
            self.logger.addHandler(logging.StreamHandler())
            self.logger.setLevel(logging.DEBUG)

        def _gen_key(self, table, data_itm):
            k = ''
            for key in self.key[table]:
                k += str(data_itm[key])
            return k

        def put_message(self, msg_txt):
            msg = json.loads(msg_txt)

            # multiplexing
            if 'subscribe' in msg:
                self.logger.info('subscribe to: %s' % msg['subscribe'])
            elif 'error' in msg:
                self.logger.error('error : %s' % msg['error'])
            elif 'table' in msg:
                table = msg['table']
                action = msg[
                    'action']  # 'partial' | 'update' | 'insert' | 'delete',
                # 테이블이 데이터 딕셔너리에 없을떄
                if table not in self.data:
                    self.data[table] = {}  # 생성

                if action == 'partial':
                    self.key[table] = msg['keys']

                if table in self.key:
                    for itm in msg['data']:
                        gen_key = self._gen_key(table, itm)

                        if action == 'delete':
                            del self.data[table][gen_key]
                        else:
                            self.data[table][gen_key] = itm
                else:
                    self.logger.error('error : %s  not in self.key' % table)
            else:
                pass

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
