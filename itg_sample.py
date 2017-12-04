from itgaecoin.itg_bitmexapi import BitmexWebsocket, BitmexAPI, BitmexUtil
from enum import Enum
import time

# 레버리지 지정
G_LEVERAGE = 10

# 상태 값
class FSM_VAL(Enum):
    CHECK_POSITION = 1
    CHECK_LINES = 2

# 상태
G_FSM = FSM_VAL.CHECK_POSITION


# 포지션 체크
def check_position(rs_api, position_json):
    """
    :param rs_api: restful api
    :type rs_api: BitmexAPI
    :param leverage:  reverage
    :return:
    """
    global  G_FSM
    global  G_LEVERAGE

    if position_json == None:
        return

    # reset
    key = list(position_json.keys())[0]
    position_json = position_json[key]

    # G_MAX_LEVERAGE값으로 맞춘다.
    if position_json['leverage'] !=  G_LEVERAGE:
        print('post! leverage to %s' % G_LEVERAGE)
        rs_api.post_position_leverage(G_LEVERAGE)

    # 다음상태 로 넘어간다.
    G_FSM = FSM_VAL.CHECK_LINES



def check_lines():
    pass



def main(apikey, secret):
    global  G_FSM

    # websocket 객체 실시간 데이터 담당
    ws_api = BitmexWebsocket(apikey, secret)

    # restful api , Rq/Rp 수행하는 객체
    rs_api = BitmexAPI(apikey, secret)

    # 데이터 딕셔너리 형태로 저장소
    rdata = BitmexUtil.WsRecentData()

    # 웹소켓 내부 메시지큐 (데이터 수신시 백그라운드스레드가 해당큐에 넣는다.)
    msg_q = ws_api.get_message_queue()

    # 웹소켓 원하는 토픽 구독후 백그라운드 스레드 실행
    ws_api.run_with_topics(BitmexUtil.normal_topics())

    # message loop
    while True:
        # 메인스레드에서 큐에서 데이터를 꺼냄( 블로킹모드 )
        txt = msg_q.get()
        # 데이터 스냅샷 저장( 내부적으로 JSON 형태로 변환)
        rdata.put_message(txt)

        if G_FSM == FSM_VAL.CHECK_POSITION:
            if 'position' in  rdata.data :
                check_position(rs_api, rdata.data['position'])
        elif G_FSM == FSM_VAL.CHECK_LINES:
            break


    ws_api.ws.close()
    time.sleep(5) # more wait

