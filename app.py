from flask import Flask, request, jsonify
from SpamReqInvApiMain import *
from SpamReqInvApiSetting import *
import threading
import time
import socket
import json
import base64
import requests
from datetime import datetime
import jwt
from google.protobuf.timestamp_pb2 import Timestamp
import errno
import select
import atexit
import os
import signal
import sys
import urllib3
from collections import deque

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
clients = {}
shutting_down = False
client_lock = threading.Lock()

shared_0500_info = {
    'got': False,
    'idT': None,
    'squad': None,
    'AutH': None
}

MASTER_ACCOUNT_ID = '4929914168'

class ConnectionMonitor:
    """مراقب ذكي للاتصالات"""
    def __init__(self):
        self.heartbeat_interval = 10  # فحص كل 10 ثواني
        self.reconnect_delay = 3  # تأخير إعادة الاتصال
        self.max_reconnect_attempts = 5
        self.monitoring_threads = {}
        self.connection_status = {}  # تتبع حالة كل حساب
        
    def start_monitoring(self, account_id, client):
        """بدء مراقبة حساب معين"""
        if account_id in self.monitoring_threads:
            return
            
        self.connection_status[account_id] = {
            'status': 'connected',
            'last_heartbeat': time.time(),
            'reconnect_attempts': 0,
            'disconnected_at': None
        }
        
        monitor_thread = threading.Thread(
            target=self._monitor_connection,
            args=(account_id, client),
            daemon=True
        )
        self.monitoring_threads[account_id] = monitor_thread
        monitor_thread.start()
        
    def _monitor_connection(self, account_id, client):
        """مراقبة مستمرة للاتصال"""
        while not shutting_down and account_id in clients:
            try:
                time.sleep(self.heartbeat_interval)
                
                if shutting_down:
                    break
                    
                # فحص حالة الاتصال
                is_connected = self._check_connection(client)
                
                if not is_connected:
                    print(f"🔴 [مراقب] تم اكتشاف انقطاع اتصال الحساب: {account_id}")
                    self.connection_status[account_id]['status'] = 'disconnected'
                    self.connection_status[account_id]['disconnected_at'] = time.time()
                    
                    # محاولة إعادة الاتصال
                    self._attempt_reconnect(account_id, client)
                else:
                    self.connection_status[account_id]['status'] = 'connected'
                    self.connection_status[account_id]['last_heartbeat'] = time.time()
                    self.connection_status[account_id]['reconnect_attempts'] = 0
                    
            except Exception as e:
                print(f"❌ [مراقب] خطأ في مراقبة الحساب {account_id}: {e}")
                
    def _check_connection(self, client):
        """فحص ما إذا كان العميل متصلاً"""
        try:
            if not client or not client.running:
                return False
                
            # فحص socket_client
            if client.socket_client:
                if not client.is_socket_connected(client.socket_client):
                    return False
                    
            # فحص clientsocket
            if client.clientsocket:
                if not client.is_socket_connected(client.clientsocket):
                    return False
                    
            # التحقق من أن أحد المقابس على الأقل متصل
            if not client.socket_client and not client.clientsocket:
                return False
                
            return True
            
        except Exception as e:
            print(f"❌ [فحص الاتصال] خطأ: {e}")
            return False
            
    def _attempt_reconnect(self, account_id, client):
        """محاولة إعادة الاتصال بشكل ذكي"""
        max_attempts = self.max_reconnect_attempts
        
        for attempt in range(1, max_attempts + 1):
            if shutting_down:
                return
                
            print(f"🔄 [إعادة اتصال] محاولة {attempt}/{max_attempts} للحساب: {account_id}")
            
            self.connection_status[account_id]['reconnect_attempts'] = attempt
            
            try:
                # إيقاف العميل الحالي
                client.stop()
                time.sleep(2)
                
                # إعادة تشغيل العميل
                client.running = True
                client.connection_attempts = 0
                
                # محاولة الاتصال من جديد
                client.run()
                
                # انتظار للتأكد من نجاح الاتصال
                time.sleep(5)
                
                # التحقق من نجاح إعادة الاتصال
                if self._check_connection(client):
                    print(f"✅ [إعادة اتصال] تمت إعادة اتصال الحساب {account_id} بنجاح في المحاولة {attempt}")
                    self.connection_status[account_id]['status'] = 'connected'
                    self.connection_status[account_id]['reconnect_attempts'] = 0
                    
                    # إعادة تشغيل المراقبة بعد نجاح الاتصال
                    self._restart_monitoring_for_client(account_id, client)
                    return
                    
            except Exception as e:
                print(f"❌ [إعادة اتصال] فشل المحاولة {attempt} للحساب {account_id}: {e}")
                
            # تأخير قبل المحاولة التالية مع زيادة تدريجية
            delay = self.reconnect_delay * attempt
            time.sleep(delay)
            
        print(f"❌ [إعادة اتصال] فشلت جميع محاولات إعادة الاتصال للحساب: {account_id}")
        
    def _restart_monitoring_for_client(self, account_id, client):
        """إعادة بدء مراقبة العميل بعد إعادة الاتصال الناجحة"""
        # إيقاف المراقبة القديمة إذا وجدت
        if account_id in self.monitoring_threads:
            self.connection_status[account_id]['status'] = 'connected'
            self.connection_status[account_id]['last_heartbeat'] = time.time()
            
    def stop_monitoring(self, account_id):
        """إيقاف مراقبة حساب معين"""
        if account_id in self.monitoring_threads:
            del self.monitoring_threads[account_id]
            if account_id in self.connection_status:
                del self.connection_status[account_id]

# إنشاء كائن المراقبة الذكي
connection_monitor = ConnectionMonitor()

class TcpBotConnectMain:
    def __init__(self, account_id, password):
        self.account_id = account_id
        self.password = password
        self.key = None
        self.iv = None
        self.socket_client = None
        self.clientsocket = None
        self.running = False
        self.connection_attempts = 0
        self.max_connection_attempts = 3
        self.AutH = None
        self.DaTa2 = None
        self.thread = None
        self.restarting = False
        self.last_activity = time.time()  # تتبع آخر نشاط
        self.connection_health = deque(maxlen=10)  # سجل صحة الاتصال
    
    def run(self):
        if shutting_down:
            return
            
        with client_lock:
            if self.restarting:
                return
            self.restarting = True
            
        self.running = True
        self.connection_attempts = 0
        
        try:
            while self.running and not shutting_down and self.connection_attempts < self.max_connection_attempts:
                try:
                    self.connection_attempts += 1
                    print(f"[{self.account_id}] محاولة الاتصال {self.connection_attempts}/{self.max_connection_attempts}")
                    self.get_tok()
                    break
                except Exception as e:
                    print(f"[{self.account_id}] Error in run: {e}")
                    if self.connection_attempts >= self.max_connection_attempts:
                        print(f"[{self.account_id}] وصل للحد الأقصى لمحاولات الاتصال. التوقف عن المحاولة.")
                        break
                    print(f"[{self.account_id}] إعادة المحاولة بعد 5 ثواني...")
                    time.sleep(5)
        finally:
            with client_lock:
                self.restarting = False
    
    def stop(self):
        self.running = False
        try:
            if self.clientsocket:
                self.clientsocket.close()
        except:
            pass
        try:
            if self.socket_client:
                self.socket_client.close()
        except:
            pass
        print(f"[{self.account_id}] Client stopped")
    
    def safe_restart(self, delay=5):
        if shutting_down or self.restarting:
            return
            
        print(f"[{self.account_id}] Restarting client in {delay} seconds...")
        self.stop()
        time.sleep(delay)
        
        restart_thread = threading.Thread(target=self.run, daemon=True)
        restart_thread.start()
    
    def update_last_activity(self):
        """تحديث وقت آخر نشاط"""
        self.last_activity = time.time()
        self.connection_health.append(True)
    
    def is_socket_connected(self, sock):
        try:
            if sock is None:
                self.connection_health.append(False)
                return False
     
            readable, writable, exceptional = select.select([sock], [sock], [sock], 0.1)
            if sock in writable:
                self.update_last_activity()
                return True
            if sock in exceptional:
                self.connection_health.append(False)
                return False
            self.connection_health.append(True)
            return True
        except Exception as e:
            self.connection_health.append(False)
            return False
    
    def check_connection_health(self):
        """فحص صحة الاتصال بناءً على السجل"""
        if len(self.connection_health) == 0:
            return True
        # إذا كان أكثر من 50% من الفحوصات الأخيرة فاشلة
        return sum(self.connection_health) / len(self.connection_health) > 0.5
    
    def sockf1(self, tok, online_ip, online_port, packet, key, iv):
        while self.running and not shutting_down:
            try:
                self.socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_client.settimeout(30)
                self.socket_client.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                
                # إعداد keepalive للكشف السريع عن انقطاع الاتصال
                self.socket_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                self.socket_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                self.socket_client.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                
                online_port = int(online_port)
                print(f"[{self.account_id}] Connecting to {online_ip}:{online_port}...")
                self.socket_client.connect((online_ip, online_port))
                print(f"[{self.account_id}] Connected to {online_ip}:{online_port}")
                self.socket_client.send(bytes.fromhex(tok))
                print(f"[{self.account_id}] Token sent successfully")
                
                self.update_last_activity()
                
                while self.running and not shutting_down and self.is_socket_connected(self.socket_client):
                    try:
                        readable, _, _ = select.select([self.socket_client], [], [], 1.0)
                        if self.socket_client in readable:
                            self.DaTa2 = self.socket_client.recv(99999)
                            if not self.DaTa2:
                                print(f"[{self.account_id}] Server closed connection gracefully")
                                break

                            self.update_last_activity()
                    
                            if '0500' in self.DaTa2.hex()[0:4] and len(self.DaTa2.hex()) > 30:
                                try:
                                    self.packet = json.loads(DeCode_PackEt(f'08{self.DaTa2.hex().split("08", 1)[1]}'))
                                    self.AutH = self.packet['5']['data']['7']['data']
                                    print(f"[{self.account_id}] 0500 packet received, AutH={self.AutH}")

                              
                                    if self.account_id == MASTER_ACCOUNT_ID:
                                        shared_0500_info['got'] = True
                                        shared_0500_info['idT'] = self.packet['5']['data']['1']['data']
                                        shared_0500_info['squad'] = self.packet['5']['data']['31']['data']
                                        shared_0500_info['AutH'] = self.AutH
                                        print(f"[{self.account_id}] Master saved 0500 info")

                               
                                    elif shared_0500_info['got']:
                                        idT = shared_0500_info['idT']
                                        sq = shared_0500_info['squad']
                                        for _ in range(3):
                                            if self.is_socket_connected(self.socket_client):
                                                self.socket_client.send(GenJoinSquadsPacket(idT, key, iv))
                                                time.sleep(0.5)
                                                self.socket_client.send(ExiT('000000', key, iv))
                                                time.sleep(0.1)
                                                self.socket_client.send(ghost_pakcet(idT, "insta:kha_led_mhd", sq, key, iv))
                                                time.sleep(0.5)

                                except Exception as parse_err:
                                    print(f"[{self.account_id}] Error parsing 0500: {parse_err}")
                                
                    except socket.timeout:
                        continue
                    except (OSError, socket.error) as e:
                        if e.errno in [errno.EBADF, errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE, errno.ETIMEDOUT]:
                            print(f"[{self.account_id}] Connection error: {e}. Reconnecting...")
                            break
                        else:
                            print(f"[{self.account_id}] Socket error: {e}. Reconnecting...")
                            break
                    except Exception as e:
                        print(f"[{self.account_id}] Unexpected error: {e}. Reconnecting...")
                        break
                        
            except socket.timeout:
                print(f"[{self.account_id}] Connection timeout, retrying...")
                break
            except (OSError, socket.error) as e:
                if e.errno in [errno.EBADF, errno.ECONNRESET, errno.ECONNABORTED, errno.ETIMEDOUT, errno.EPIPE]:
                    print(f"[{self.account_id}] Connection error: {e}")
                else:
                    print(f"[{self.account_id}] Connection error: {e}")
                break
            except Exception as e:
                print(f"[{self.account_id}] Unexpected error: {e}")
                break
            finally:
                try:
                    if self.socket_client:
                        self.socket_client.close()
                except:
                    pass
            
            if self.running and not shutting_down:
                print(f"[{self.account_id}] Reconnecting in 3 seconds...")
                time.sleep(3)
    
    def connect(self, tok, packet, key, iv, whisper_ip, whisper_port, online_ip, online_port):
        while self.running and not shutting_down:
            try:
                self.clientsocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.clientsocket.settimeout(30)
                
                # إعداد keepalive
                self.clientsocket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                self.clientsocket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 10)
                self.clientsocket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 5)
                self.clientsocket.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
                
                print(f"[{self.account_id}] Connecting to whisper {whisper_ip}:{whisper_port}...")
                self.clientsocket.connect((whisper_ip, int(whisper_port)))
                print(f"[{self.account_id}] Connected to {whisper_ip}:{whisper_port}")
                self.clientsocket.send(bytes.fromhex(tok))
                self.data = self.clientsocket.recv(1024)
                self.clientsocket.send(get_packet2(self.key, self.iv))

                self.update_last_activity()

                socket_thread = threading.Thread(
                    target=self.sockf1,
                    args=(tok, online_ip, online_port, "anything", key, iv),
                    daemon=True
                )
                socket_thread.start()
                
               
                while self.running and not shutting_down:
                    try:
                        dataS = self.clientsocket.recv(1024)
                        if not dataS:
                            print(f"[{self.account_id}] Whisper connection closed")
                            break
                        self.update_last_activity()
                    except socket.timeout:
                        continue
                    except (OSError, socket.error) as e:
                        if e.errno in [errno.EBADF, errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE]:
                            print(f"[{self.account_id}] Whisper error: {e}")
                            break
                        else:
                            print(f"[{self.account_id}] Whisper error: {e}")
                            break
                            
            except socket.timeout:
                print(f"[{self.account_id}] Whisper connection timeout")
            except (OSError, socket.error) as e:
                if e.errno in [errno.EBADF, errno.ECONNRESET, errno.ECONNABORTED, errno.ETIMEDOUT, errno.EPIPE]:
                    print(f"[{self.account_id}] Whisper connection error: {e}")
                else:
                    print(f"[{self.account_id}] Whisper connection error: {e}")
            except Exception as e:
                print(f"[{self.account_id}] Unexpected whisper error: {e}")
            finally:
                if self.clientsocket:
                    try:
                        self.clientsocket.close()
                    except:
                        pass
                
          
            if self.running and not shutting_down:
                print(f"[{self.account_id}] Reconnecting whisper in 5 seconds...")
                time.sleep(5)

    # ... [باقي الدوال كما هي بدون تغيير] ...
    def parse_my_message(self, serialized_data):
        MajorLogRes = MajorLoginRes() 
        MajorLogRes.ParseFromString(serialized_data)
        timestamp = MajorLogRes.kts
        key = MajorLogRes.ak
        iv = MajorLogRes.aiv
        BASE64_TOKEN = MajorLogRes.token
        timestamp_obj = Timestamp()
        timestamp_obj.FromNanoseconds(timestamp)
        timestamp_seconds = timestamp_obj.seconds
        timestamp_nanos = timestamp_obj.nanos
        combined_timestamp = timestamp_seconds * 1_000_000_000 + timestamp_nanos
        return combined_timestamp, key, iv, BASE64_TOKEN
    
    def GET_PAYLOAD_BY_DATA(self, JWT_TOKEN, NEW_ACCESS_TOKEN, date):
        token_payload_base64 = JWT_TOKEN.split('.')[1]
        token_payload_base64 += '=' * ((4 - len(token_payload_base64) % 4) % 4)
        decoded_payload = base64.urlsafe_b64decode(token_payload_base64).decode('utf-8')
        decoded_payload = json.loads(decoded_payload)
        NEW_EXTERNAL_ID = decoded_payload['external_id']
        SIGNATURE_MD5 = decoded_payload['signature_md5']
        now = datetime.now()
        now = str(now)[:len(str(now))-7]
        formatted_time = date
        payload = bytes.fromhex(Payload1A13)
        payload = payload.replace(b"2025-07-30 11:02:51", str(now).encode())
        payload = payload.replace(b"c69ae208fad72738b674b2847b50a3a1dfa25d1a19fae745fc76ac4a0e414c94", NEW_ACCESS_TOKEN.encode("UTF-8"))
        payload = payload.replace(b"4306245793de86da425a52caadf21eed", NEW_EXTERNAL_ID.encode("UTF-8"))
        payload = payload.replace(b"7428b253defc164018c604a1ebbfebdf", SIGNATURE_MD5.encode("UTF-8"))
        PAYLOAD = payload.hex()
        PAYLOAD = encrypt_api(PAYLOAD)
        PAYLOAD = bytes.fromhex(PAYLOAD)
        whisper_ip, whisper_port, online_ip, online_port = self.GET_LOGIN_DATA(JWT_TOKEN , PAYLOAD)
        return whisper_ip, whisper_port, online_ip, online_port
    
    def GET_LOGIN_DATA(self, JWT_TOKEN, PAYLOAD):
        url = GetLoginDataRegionMena
        headers = {
            'Expect': '100-continue',
            'Authorization': f'Bearer {JWT_TOKEN}',
            'X-Unity-Version': '2018.4.11f1',
            'X-GA': 'v1 1',
            'ReleaseVersion': FreeFireVersion,
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 9; G011A Build/PI)',
            'Host': 'clientbp.common.ggbluefox.com',
            'Connection': 'close',
            'Accept-Encoding': 'gzip, deflate, br',
        }
        
        max_retries = 3
        attempt = 0
        while attempt < max_retries and not shutting_down:
            try:
                response = requests.post(url, headers=headers, data=PAYLOAD, verify=False)
                response.raise_for_status()
                x = response.content.hex()
                json_result = get_available_room(x)
                parsed_data = json.loads(json_result)
                whisper_address = parsed_data['32']['data']
                online_address = parsed_data['14']['data']
                online_ip = online_address[:len(online_address) - 6]
                whisper_ip = whisper_address[:len(whisper_address) - 6]
                online_port = int(online_address[len(online_address) - 5:])
                whisper_port = int(whisper_address[len(whisper_address) - 5:])
                return whisper_ip, whisper_port, online_ip, online_port
            except requests.RequestException as e:
                print(f"[{self.account_id}] Request failed: {e}. Attempt {attempt + 1} of {max_retries}. Retrying...")
                attempt += 1
                time.sleep(2)
        print(f"[{self.account_id}] Failed to get login data after multiple attempts.")
        return None, None, None, None
    
    def guest_token(self, uid, password):
        url = "https://100067.connect.garena.com/oauth/guest/token/grant"
        headers = {
            "Host": "100067.connect.garena.com",
            "User-Agent": "GarenaMSDK/4.0.19P4(G011A ;Android 10;en;EN;)",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "close",
        }
        data = {
            "uid": f"{uid}",
            "password": f"{password}",
            "response_type": "token",
            "client_type": "2",
            "client_secret": "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
            "client_id": "100067",
        }
        response = requests.post(url, headers=headers, data=data)
        data = response.json()
        NEW_ACCESS_TOKEN = data['access_token'] 
        NEW_OPEN_ID = data['open_id']
        
        OLD_ACCESS_TOKEN = "c69ae208fad72738b674b2847b50a3a1dfa25d1a19fae745fc76ac4a0e414c94"
        OLD_OPEN_ID = "4306245793de86da425a52caadf21eed"

        time.sleep(0.2)
        data = self.TOKEN_MAKER(OLD_ACCESS_TOKEN, NEW_ACCESS_TOKEN, OLD_OPEN_ID, NEW_OPEN_ID, uid)
        return data
        
    def TOKEN_MAKER(self, OLD_ACCESS_TOKEN, NEW_ACCESS_TOKEN, OLD_OPEN_ID, NEW_OPEN_ID, id):
        headers = {
            'X-Unity-Version': '2018.4.11f1',
            'ReleaseVersion': FreeFireVersion,
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-GA': 'v1 1',
            'Content-Length': '928',
            'User-Agent': 'Dalvik/2.1.0 (Linux; U; Android 7.1.2; ASUS_Z01QD Build/QKQ1.190825.002)',
            'Host': 'loginbp.common.ggbluefox.com',
            'Connection': 'Keep-Alive',
            'Accept-Encoding': 'gzip'
        }
        
        data = bytes.fromhex(Payload1A13)
        data = data.replace(OLD_OPEN_ID.encode(), NEW_OPEN_ID.encode())
        data = data.replace(OLD_ACCESS_TOKEN.encode(), NEW_ACCESS_TOKEN.encode())
        hex_data = data.hex()
        encrypted_data = encrypt_api(hex_data)
        Final_Payload = bytes.fromhex(encrypted_data)
        
        URL = MajorLoginRegionMena
        RESPONSE = requests.post(URL, headers=headers, data=Final_Payload, verify=False)
        
        if RESPONSE.status_code == 200:
            if len(RESPONSE.content) < 10:
                return False
                
            combined_timestamp, key, iv, BASE64_TOKEN = self.parse_my_message(RESPONSE.content)
            whisper_ip, whisper_port, online_ip, online_port = self.GET_PAYLOAD_BY_DATA(BASE64_TOKEN, NEW_ACCESS_TOKEN, 1)
            self.key = key
            self.iv = iv
            print(f"[{self.account_id}] Key: {key}, IV: {iv}")
            return (BASE64_TOKEN, key, iv, combined_timestamp, whisper_ip, whisper_port, online_ip, online_port)
        else:
            return False
    
    def get_tok(self):
        token_data = self.guest_token(self.account_id, self.password)
        if not token_data:
            print(f"[{self.account_id}] Failed to get token")
            self.safe_restart()
            return
        
        token, key, iv, Timestamp, whisper_ip, whisper_port, online_ip, online_port = token_data
        print(f"[{self.account_id}] Whisper: {whisper_ip}:{whisper_port}")
        
        try:
            decoded = jwt.decode(token, options={"verify_signature": False})
            account_id = decoded.get('account_id')
            encoded_acc = hex(account_id)[2:]
            hex_value = self.dec_to_hex(Timestamp)
            time_hex = hex_value
            BASE64_TOKEN_ = token.encode().hex()
            print(f"[{self.account_id}] Token decoded. Account ID: {account_id}")
        except Exception as e:
            print(f"[{self.account_id}] Error processing token: {e}")
            self.safe_restart()
            return
        
        try:
            head = hex(len(encrypt_packet(BASE64_TOKEN_, key, iv)) // 2)[2:]
            length = len(encoded_acc)
            zeros = '00000000'
            if length == 9:
                zeros = '0000000'
            elif length == 8:
                zeros = '00000000'
            elif length == 10:
                zeros = '000000'
            elif length == 7:
                zeros = '000000000'
            else:
                print(f"[{self.account_id}] Unexpected length encountered")
            head = f'0115{zeros}{encoded_acc}{time_hex}00000{head}'
            final_token = head + encrypt_packet(BASE64_TOKEN_, key, iv)
        except Exception as e:
            print(f"[{self.account_id}] Error creating final token: {e}")
            self.safe_restart()
            return
        
        self.connect(final_token, 'anything', key, iv, whisper_ip, whisper_port, online_ip, online_port)
        return final_token, key, iv
    
    def dec_to_hex(self, ask):
        ask_result = hex(ask)
        final_result = str(ask_result)[2:]
        if len(final_result) == 1:
            final_result = "0" + final_result
            return final_result
        else:
            return final_result
    
    def execute_command(self, command, *args):
        global shared_0500_info

        if '/OTMAN' in command[:7]:
            try:
                if not self.socket_client or not self.is_socket_connected(self.socket_client):
                    self.safe_restart(delay=2)
                    return "Socket not connected, attempting restart..."
                
                team_code = args[0] if len(args) > 0 else None
                account_name = args[1] if len(args) > 1 else f"Ghost_{self.account_id}"

                if not team_code:
                    return "No team code provided for /OTMAN"

                self.id = team_code
                self.nm = account_name
                print(f"[{self.account_id}] Executing /OTMAN for team code {self.id} with name {self.nm}")

                if self.account_id == MASTER_ACCOUNT_ID:
                    shared_0500_info['got'] = False
                    shared_0500_info['idT'] = None
                    shared_0500_info['squad'] = None
                    shared_0500_info['AutH'] = None

                    got_0500 = False
                    attempt_counter = 0
                    max_attempts = 3  

                    while not got_0500 and attempt_counter < max_attempts:
                        attempt_counter += 1
                        print(f"[{self.account_id}] Attempt {attempt_counter}/{max_attempts} joining/exiting squad {self.id}...")

                        if self.is_socket_connected(self.socket_client):
                            self.socket_client.send(GenJoinSquadsPacket(self.id, self.key, self.iv))
                            time.sleep(0.5)
                            self.socket_client.send(ExiT('000000', self.key, self.iv))
                            time.sleep(0.1)

                            time.sleep(0.5)
                            
                            if shared_0500_info['got']:
                                idT = shared_0500_info['idT']
                                sq = shared_0500_info['squad']
                                print(f"[{self.account_id}] Got 0500 with ID: {idT}")

                                if self.is_socket_connected(self.socket_client):
                                    self.socket_client.send(ExiT('000000', self.key, self.iv))
                                    time.sleep(0.2)
                                    for _ in range(2):
                                        self.socket_client.send(ghost_pakcet(idT, self.nm, sq, self.key, self.iv))
                                        time.sleep(0.5)
                                    self.socket_client.send(ExiT('000000', self.key, self.iv))
                                    time.sleep(0.2)
                                    got_0500 = True
                    
                    if not got_0500:
                        print(f"[{self.account_id}] ❌ Failed to get 0500 for team code {self.id} after {attempt_counter} attempts - STOPPING")
                        return f"❌ Failed to join team {self.id} after {attempt_counter} attempts"
                    
                    print(f"[{self.account_id}] ✅ Successfully joined team {self.id} after {attempt_counter} attempts")
                    return f"✅ /OTMAN master command executed successfully after {attempt_counter} attempts"

                else:
                
                    wait_attempts = 0
                    max_wait_attempts = 3 
                    
                    while not shared_0500_info['got'] and wait_attempts < max_wait_attempts:
                        wait_attempts += 1
                        print(f"[{self.account_id}] Waiting for master 0500... Attempt {wait_attempts}/{max_wait_attempts}")
                        time.sleep(0.5)

                    if not shared_0500_info['got']:
                        print(f"[{self.account_id}] ❌ Timeout waiting for master account after {wait_attempts} attempts - STOPPING")
                        return "❌ Timeout waiting for master account to get 0500"

                    idT = shared_0500_info['idT']
                    sq = shared_0500_info['squad']
                    
                
                    ghost_attempts = 0
                    max_ghost_attempts = 3
                    ghost_success = False
                    
                    while not ghost_success and ghost_attempts < max_ghost_attempts:
                        ghost_attempts += 1
                        print(f"[{self.account_id}] Sending ghost packets... Attempt {ghost_attempts}/{max_ghost_attempts}")
                        
                        if self.is_socket_connected(self.socket_client):
                            try:
                                self.socket_client.send(GenJoinSquadsPacket(idT, self.key, self.iv))
                                time.sleep(0.5)
                                self.socket_client.send(ExiT('000000', self.key, self.iv))
                                time.sleep(0.1)
                                self.socket_client.send(ghost_pakcet(idT, self.nm, sq, self.key, self.iv))
                                time.sleep(0.5)
                                self.socket_client.send(ExiT('000000', self.key, self.iv))
                                ghost_success = True
                                print(f"[{self.account_id}] ✅ Ghost packets sent successfully")
                            except Exception as send_error:
                                print(f"[{self.account_id}] ❌ Error sending ghost packets attempt {ghost_attempts}: {send_error}")
                        
                        if not ghost_success and ghost_attempts < max_ghost_attempts:
                            time.sleep(0.5)
                    
                    if not ghost_success:
                        print(f"[{self.account_id}] ❌ Failed to send ghost packets after {ghost_attempts} attempts - STOPPING")
                        return f"❌ Failed to execute ghost command after {ghost_attempts} attempts"
                    
                    print(f"[{self.account_id}] ✅ /OTMAN ghost command executed successfully using master data")
                    return f"✅ /OTMAN ghost command executed successfully"

            except Exception as e:
                print(f"[{self.account_id}] ❌ Error in execute_command: {e}")
                return f"❌ Error executing command: {e}"
        else:
            return f"❌ Unknown command: {command}"

def load_accounts(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Accounts file {file_path} not found")
        return {}
    except json.JSONDecodeError:
        print(f"Invalid JSON in accounts file {file_path}")
        return {}

def cleanup():
    global shutting_down
    shutting_down = True
    print("Shutting down all clients...")
    
    # إيقاف المراقبة لجميع الحسابات
    for account_id in list(clients.keys()):
        connection_monitor.stop_monitoring(account_id)
    
    for account_id, client in list(clients.items()):
        client.stop()
    print("Cleanup completed")

# Routes Flask
@app.route('/start_client', methods=['GET'])
def start_client():
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    account_id = request.args.get('account_id')
    password = request.args.get('password')

    if not account_id or not password:
        return jsonify({'error': 'Account ID and password are required'}), 400

    if account_id in clients:
        return jsonify({'error': 'Client already running'}), 400

    client = TcpBotConnectMain(account_id, password)
    clients[account_id] = client

    client_thread = threading.Thread(target=client.run, daemon=True)
    client_thread.start()
    
    # بدء المراقبة الذكية للحساب الجديد
    time.sleep(2)  # انتظار حتى يبدأ الاتصال
    connection_monitor.start_monitoring(account_id, client)

    return jsonify({'message': f'Client {account_id} started successfully with smart monitoring'}), 200

@app.route('/stop_client', methods=['GET'])
def stop_client():
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    account_id = request.args.get('account_id')

    if not account_id:
        return jsonify({'error': 'Account ID is required'}), 400

    if account_id not in clients:
        return jsonify({'error': 'Client not found'}), 404

    # إيقاف المراقبة أولاً
    connection_monitor.stop_monitoring(account_id)
    
    client = clients[account_id]
    client.stop()
    del clients[account_id]

    return jsonify({'message': f'Client {account_id} stopped successfully'}), 200

@app.route('/connection_status', methods=['GET'])
def get_connection_status():
    """API لفحص حالة الاتصال لجميع الحسابات"""
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    account_id = request.args.get('account_id')
    
    if account_id:
        # حالة حساب محدد
        if account_id not in connection_monitor.connection_status:
            return jsonify({'error': 'Account not found'}), 404
            
        status = connection_monitor.connection_status[account_id]
        client = clients.get(account_id)
        
        return jsonify({
            'account_id': account_id,
            'status': status,
            'is_connected': connection_monitor._check_connection(client) if client else False,
            'last_heartbeat': time.time() - status.get('last_heartbeat', 0),
            'reconnect_attempts': status.get('reconnect_attempts', 0)
        }), 200
    else:
        # حالة جميع الحسابات
        all_status = {}
        for aid in connection_monitor.connection_status:
            client = clients.get(aid)
            status = connection_monitor.connection_status[aid]
            all_status[aid] = {
                'status': status['status'],
                'is_connected': connection_monitor._check_connection(client) if client else False,
                'last_heartbeat': time.time() - status.get('last_heartbeat', 0),
                'reconnect_attempts': status.get('reconnect_attempts', 0)
            }
        
        return jsonify({
            'total_accounts': len(all_status),
            'connected_accounts': sum(1 for s in all_status.values() if s['is_connected']),
            'disconnected_accounts': sum(1 for s in all_status.values() if not s['is_connected']),
            'accounts': all_status
        }), 200

@app.route('/reconnect_account', methods=['GET'])
def reconnect_account():
    """API يدوي لإعادة اتصال حساب معين"""
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    account_id = request.args.get('account_id')

    if not account_id:
        return jsonify({'error': 'Account ID is required'}), 400

    if account_id not in clients:
        return jsonify({'error': 'Account not found'}), 404

    client = clients[account_id]
    
    # إعادة الاتصال يدوياً
    print(f"🔄 [يدوي] بدء إعادة اتصال الحساب: {account_id}")
    connection_monitor._attempt_reconnect(account_id, client)
    
    return jsonify({
        'message': f'Reconnection initiated for account {account_id}',
        'account_id': account_id,
        'status': connection_monitor.connection_status.get(account_id, {})
    }), 200

@app.route('/health_check', methods=['GET'])
def health_check():
    """فحص صحة الخادم وجميع الحسابات"""
    if shutting_down:
        return jsonify({'status': 'shutting_down'}), 503

    total_accounts = len(clients)
    connected_accounts = 0
    
    for aid, client in clients.items():
        if connection_monitor._check_connection(client):
            connected_accounts += 1
    
    return jsonify({
        'server_status': 'running',
        'total_accounts': total_accounts,
        'connected_accounts': connected_accounts,
        'disconnected_accounts': total_accounts - connected_accounts,
        'uptime': 'active'
    }), 200

@app.route('/execute_command', methods=['GET'])
def execute_command_api():
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    account_id = request.args.get('account_id')
    command = request.args.get('command')
    client_id = request.args.get('client_id')

    if not account_id or not command:
        return jsonify({'error': 'Account ID and command are required'}), 400

    if account_id not in clients:
        return jsonify({'error': 'Client not found'}), 404

    client = clients[account_id]

    args = []
    if client_id:
        try:
            args.append(client_id)
        except ValueError:
            return jsonify({'error': 'Invalid client_id format'}), 400
            
    if command.startswith("/OTMAN"):
        if "=" in command:
            cmd, arg = command.split("=", 1)
        else:
            parts = command.split(" ", 1)
            cmd = parts[0]
            arg = parts[1] if len(parts) > 1 else None

        if cmd == "/OTMAN" and arg:
            account_name = request.args.get('account_name', str(account_id)) 
            result = client.execute_command(cmd, arg, account_name)
            return jsonify({'result': result}), 200

    result = client.execute_command(command, *args)
    return jsonify({'result': result}), 200

@app.route('/list_clients', methods=['GET'])
def list_clients():
    client_list = {}
    for aid, client in clients.items():
        is_connected = connection_monitor._check_connection(client)
        client_list[aid] = {
            'connected': is_connected,
            'status': connection_monitor.connection_status.get(aid, {}).get('status', 'unknown')
        }
    return jsonify({'clients': client_list}), 200

@app.route('/execute_command_all', methods=['GET'])
def execute_command_all():
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    command = request.args.get('command')
    if not command:
        return jsonify({'error': 'Command parameter is required'}), 400

    if "=" in command:
        cmd, arg = command.split("=", 1)
    else:
        parts = command.split(" ", 1)
        cmd = parts[0]
        arg = parts[1] if len(parts) > 1 else None

    ghost_names = {
        "4929576204": "OTMAN KING",
        "4929588971": "OTMAN KING", 
        "4929590365": "OTMAN KING",
        "4929635847": "OTMAN KING",
        "4929635932": "OTMAN KING"
    }

    results = {}
    
    master_client = clients.get(MASTER_ACCOUNT_ID)
    if master_client and cmd == "/OTMAN" and arg:
        master_name = ghost_names.get(MASTER_ACCOUNT_ID, MASTER_ACCOUNT_ID)
        master_result = master_client.execute_command(cmd, arg, master_name)
        results[MASTER_ACCOUNT_ID] = f"MASTER: {master_result} | Name: {master_name}"
        time.sleep(1)
        
    for account_id, client in clients.items():
        if account_id != MASTER_ACCOUNT_ID:
            account_name = ghost_names.get(str(account_id), str(account_id))
            if cmd == "/OTMAN" and arg:
                result = client.execute_command(cmd, arg, account_name)
                results[account_id] = f"GHOST: {result} | Name: {account_name}"

    return jsonify({'results': results})

@app.route('/ghost', methods=['GET'])
def ghost_command():
    if shutting_down:
        return jsonify({'error': 'Server is shutting down'}), 503

    name = request.args.get('name')
    team_code = request.args.get('team_code')

    if not team_code:
        return jsonify({'error': 'team_code parameter is required'}), 400

    ghost_names = {
        "4929576204": "OTMAN KING",
        "4929588971": "OTMAN KING", 
        "4929590365": "OTMAN KING",
        "4929635847": "OTMAN KING",
        "4929635932": "OTMAN KING"
    }

    results = {}

    master_client = clients.get(MASTER_ACCOUNT_ID)
    if master_client:
        master_name = ghost_names.get(MASTER_ACCOUNT_ID, MASTER_ACCOUNT_ID)
        master_result = master_client.execute_command("/OTMAN", team_code, master_name)
        results[MASTER_ACCOUNT_ID] = f"MASTER: {master_result} | Name: {master_name}"
        time.sleep(1)
        
    for account_id, client in clients.items():
        if account_id != MASTER_ACCOUNT_ID:
            account_name = name if name else ghost_names.get(str(account_id), str(account_id))
            result = client.execute_command("/OTMAN", team_code, account_name)
            results[account_id] = f"GHOST: {result} | Name: {account_name}"

    return jsonify({
        'command': 'ghost',
        'team_code': team_code,
        'master_name': ghost_names.get(MASTER_ACCOUNT_ID, MASTER_ACCOUNT_ID),
        'ghost_names_used': 'custom' if name else 'default_account_names',
        'results': results
    }), 200

@app.route('/shutdown', methods=['GET'])
def shutdown_server():
    global shutting_down
    shutting_down = True
    cleanup()
    return jsonify({'message': 'Server shutdown initiated'}), 200

def signal_handler(sig, frame):
    print('Received shutdown signal')
    cleanup()
    sys.exit(0)

def find_available_port(start_port=14008, max_attempts=10):
    for port in range(start_port, start_port + max_attempts):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('0.0.0.0', port))
                return port
        except OSError:
            continue
    return start_port

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    atexit.register(cleanup)

    try:
        accounts = load_accounts('accounts.json')
        for account_id, password in accounts.items():
            client = TcpBotConnectMain(account_id, password)
            clients[account_id] = client
            client_thread = threading.Thread(target=client.run, daemon=True)
            client_thread.start()
            time.sleep(2)
            
            # بدء المراقبة الذكية لكل حساب
            connection_monitor.start_monitoring(account_id, client)
            
    except Exception as e:
        print(f"Error loading accounts: {e}")

    try:
        port = find_available_port()
        print(f"🚀 Starting server on port {port} with smart connection monitoring")
        print("📊 المميزات الجديدة:")
        print("   ✅ مراقبة ذكية لكل حساب")
        print("   ✅ إعادة اتصال تلقائي عند الانقطاع")
        print("   ✅ تتبع حالة الاتصال لكل حساب")
        print("   ✅ API لفحص حالة الاتصالات")
        print("   ✅ إمكانية إعادة الاتصال يدوياً")
        print("   ✅ فحص صحة الخادم")
        print(f"   📡 APIs المتاحة على: http://0.0.0.0:{port}")
        print("      - /connection_status - فحص حالة الاتصال")
        print("      - /reconnect_account - إعادة اتصال يدوي")
        print("      - /health_check - فحص صحة الخادم")
        print("      - /start_client - بدء حساب جديد")
        print("      - /stop_client - إيقاف حساب")
        print("      - /list_clients - قائمة الحسابات مع حالتها")
        
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except Exception as e:
        print(f"Server error: {e}")
        cleanup()