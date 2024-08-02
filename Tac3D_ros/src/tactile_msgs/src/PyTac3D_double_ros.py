import numpy as np
import time
import struct
import queue
import ruamel.yaml
import socket
import threading
import cv2
import rospy
from tactile_msgs.msg import TactileMsg
from tactile_msgs.msg import Frame


class UDP_Manager:
    def __init__(self, callback, isServer = False, ip = '', port = 8083, frequency = 50, inet = 4):
        self.callback = callback
        
        self.isServer = isServer
        self.interval = 1.0 / frequency

        # self.available_addr = socket.getaddrinfo(socket.gethostname(), port)
        # self.hostname = socket.getfqdn(socket.gethostname())
        self.inet = inet
        self.af_inet = None
        self.ip = ip
        self.localIp = None
        self.port = port
        self.addr = (self.ip, self.port)
        self.running = False

        # self.serialNum = 0
        # self.recvPools = {} #{'IP:PORT': [{serialNum:[data..., recvNum, packetNum, timestamp]}]}

    def start(self):
        if self.inet == 4:
            self.af_inet = socket.AF_INET  # ipv4
            self.localIp = '127.0.0.1'
        elif self.inet == 6:
            self.af_inet = socket.AF_INET6 # ipv6
            self.localIp = '::1'
        self.sockUDP = socket.socket(self.af_inet, socket.SOCK_DGRAM)

        if self.isServer:
            self.roleName = 'Server'
        else:
            self.port = 0
            self.roleName = 'Client'
        
        self.sockUDP.bind((self.ip, self.port))
        self.sockUDP.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 212992)
        self.addr = self.sockUDP.getsockname()
        self.ip = self.addr[0]
        self.port = self.addr[1]
        print(self.roleName, '(UDP) at:', self.ip, ':', self.port)
        
        self.running = True
        self.thread = threading.Thread(target = self.receive, args=())
        self.thread.setDaemon(True)
        self.thread.start()  #打开收数据的线程
        
    # def ListAddr(self):
    #     for item in self.available_addr:
    #         if item[0] == self.af_inet:
    #             print(item[4])
    
    def receive(self):
        while self.running:
            time.sleep(self.interval)
            while self.running:
                try:
                    recvData, recvAddr = self.sockUDP.recvfrom(65535) #等待接受数据
                except:
                    break
                if not recvData:
                    break
                self.callback(recvData, recvAddr)
            
    def send(self, data, addr):
        self.sockUDP.sendto(data, addr)

    def close(self):
        self.running = False

class Sensor_ros1:
    def __init__(self, port = 9988, maxQSize = 8):
        '''
        Parameters
        ----------
        recvCallback: 回调函数
            recvCallback(frame)，接收一个参数frame，frame为触觉数据帧。
            在每次收到一个完整的Tac3D-Desktop传来的数据帧后，将自动调用
            一次该回调函数。回调函数的参数为所接收到的触觉数据帧。
            数据帧frame的数据结构介绍见Sensor.getFrame()的说明部分
        port: 整形
            接收Tac3D-Desktop传来的数据的UDP端口。需要注意此端口应与
            在Tac3D-Desktop中设置的数据接收端口一致。
        maxQSize: 整形
            最大接收队列长度。在使用Sensor.getFrame()获取触觉数据帧时，
            数据帧缓存队列的最大长度。（若使用recvCallback方法获取触觉
            数据帧，则不受此参数的影响）
        '''
        rospy.init_node('tactile_frame_publisher', anonymous=True)
        self.frame_pub = rospy.Publisher('tactile_frame',TactileMsg, queue_size=8)
        self.rate = rospy.Rate(30)
        self.frame_msg = TactileMsg()
        self.sensor_left_msg = Frame()
        self.sensor_right_msg = Frame()
        
        self._UDP = UDP_Manager(self._recvCallback_UDP, isServer = True, port = port)
        self.l_frame = queue.Queue()
        self.r_frame = queue.Queue()
        self._recvBuffer = {}
        self._maxQSize = maxQSize

        self._count = 0
        self._yaml = ruamel.yaml.YAML()
        self._startTime = time.time()
        self._UDP.start()
        self.left_recvFlag = False
        self.right_recvFlag = False
        self._fromAddrMap = {}

        self.left_SN = None
        self.right_SN = None

        
    def _recvCallback_UDP(self, data, addr):
        
        serialNum, pktNum, pktCount = struct.unpack('=IHH', data[0:8])
        currBuffer = self._recvBuffer.get(serialNum)
        if currBuffer is None:
            currBuffer = [0.0, pktNum, 0, [None]*(pktNum+1)]
            self._recvBuffer[serialNum] = currBuffer
        currBuffer[0] = time.time()
        currBuffer[2] += 1
        currBuffer[3][pktCount] = data[8:]
        
        if currBuffer[2] == currBuffer[1]+1:
            try:
                frame = self._decodeFrame(currBuffer[3][0], b''.join(currBuffer[3][1:]))
                initializeProgress = frame.get('InitializeProgress')
                if initializeProgress != None:
                    if initializeProgress != 100:
                        return
            except:
                return

            self._fromAddrMap[frame['SN']] = addr
            
            if frame['SN'][-1] == 'L':
                if self.l_frame.empty():
                    self.left_SN = frame['SN']
                if not self.l_frame.full():
                    self.l_frame.put(frame)
                    if self.l_frame.qsize() > self._maxQSize: 
                        self.l_frame.get()
                self.left_recvFlag = True
            elif frame['SN'][-1] == 'R':
                if self.r_frame.empty():
                    self.right_SN = frame['SN']
                if not self.r_frame.full():
                    self.r_frame.put(frame)   
                    if self.r_frame.qsize() > self._maxQSize: 
                        self.r_frame.get()
                self.right_recvFlag = True
            

            del self._recvBuffer[serialNum]
        
        self._count += 1
        if self._count > 2000:
            self._cleanBuffer()
            self._count = 0
        
    def _decodeFrame(self, headBytes, dataBytes):
        #print(headBytes)
        #print(len(dataBytes))
        #print(headBytes.decode('ascii'))
        head = self._yaml.load(headBytes.decode('ascii'))
        frame = {}
        frame['index'] = head['index']
        frame['SN'] = head['SN']
        frame['sendTimestamp'] = head['timestamp']
        frame['recvTimestamp'] = time.time() - self._startTime
        for item in head['data']:
            dataType = item['type']
            if dataType == 'mat':
                dtype = item['dtype']
                if dtype == 'f64':
                    width = item['width']
                    height = item['height']
                    offset = item['offset']
                    length = item['length']
                    frame[item['name']] = np.frombuffer(dataBytes[offset:offset+length], dtype=np.float64).reshape([height, width])
            elif dataType == 'f64':
                offset = item['offset']
                length = item['length']
                frame[item['name']] = struct.unpack('d', dataBytes[offset:offset+length])[0]
            elif dataType == 'i32':
                offset = item['offset']
                length = item['length']
                frame[item['name']] = struct.unpack('i', dataBytes[offset:offset+length])[0]
            elif dataType == 'img':
                offset = item['offset']
                length = item['length']
                frame[item['name']] = cv2.imdecode(np.frombuffer(dataBytes[offset:offset+length], np.uint8), cv2.IMREAD_ANYCOLOR)
        return frame
        
    def _cleanBuffer(self, timeout = 5):
        currTime = time.time()
        delList = []
        for item in self._recvBuffer.items():
            if currTime - item[1][0] > timeout:
                delList.append(item[0])
        for item in delList:
            #print(self._recvBuffer[item][0:3])
            del self._recvBuffer[item]
    
    def data_pub(self,l_data,r_data):
        self.frame_msg.header.stamp = rospy.Time.now()
        self.sensor_left_msg.SN = l_data['SN']
        self.sensor_left_msg.index = l_data['index']
        self.sensor_left_msg.Positions = l_data['3D_Positions'].flatten().tolist()
        self.sensor_left_msg.Displacements = l_data['3D_Displacements'].flatten().tolist()
        self.sensor_left_msg.Forces = l_data['3D_Forces'].flatten().tolist()
        self.sensor_left_msg.ResultantForce = l_data['3D_ResultantForce'].flatten().tolist()
        self.sensor_left_msg.ResultantMoment = l_data['3D_ResultantMoment'].flatten().tolist()
        self.sensor_right_msg.SN = r_data['SN']
        self.sensor_right_msg.index = r_data['index']
        self.sensor_right_msg.Positions = r_data['3D_Positions'].flatten().tolist()
        self.sensor_right_msg.Displacements = r_data['3D_Displacements'].flatten().tolist()
        self.sensor_right_msg.Forces = r_data['3D_Forces'].flatten().tolist()
        self.sensor_right_msg.ResultantForce = r_data['3D_ResultantForce'].flatten().tolist()
        self.sensor_right_msg.ResultantMoment = r_data['3D_ResultantMoment'].flatten().tolist()
        self.frame_msg.left = self.sensor_left_msg
        self.frame_msg.right = self.sensor_right_msg
        
        self.frame_pub.publish(self.frame_msg)        
        
    def syn_pub(self):
        while not rospy.is_shutdown():
            if not self.l_frame.empty() and not self.r_frame.empty():
                l_data = self.l_frame.get()
                r_data = self.r_frame.get()
                
                time_diff = abs(l_data['recvTimestamp']-r_data['recvTimestamp'])

                if time_diff <= 0.055:
                    self.data_pub(l_data,r_data)
                    
                else:

                    if l_data['recvTimestamp'] < r_data['recvTimestamp']:
                        if not self.l_frame.empty():
                            l_data = self.l_frame.get()
                            if abs(l_data['recvTimestamp']-r_data['recvTimestamp']) <= 0.055:
                                self.data_pub(l_data,r_data)
                    else:
                        if not self.r_frame.empty():
                            r_data = self.r_frame.get()     
                            if abs(l_data['recvTimestamp']-r_data['recvTimestamp']) <= 0.055:
                                self.data_pub(l_data,r_data)
                            
                        
    def ros_pub_start(self):
        self.pub_thread = threading.Thread(target = self.syn_pub, args=())
        self.pub_thread.setDaemon(True)
        self.pub_thread.start()  #打开收数据的线程     
                            
    
    def waitForFrame(self):
        '''
        阻塞等待接收数据帧，直到接收到第一帧数据帧为止。需注意，此函数的
        用途通常为等待Tac3D-Desktop启动传感器，而非在接收到一帧的数据后
        等待下一帧数据。若在调用此函数之前已经成功接收到来自Tac3D-Desktop
        的数据，则在此函数中不会进行任何阻塞等待。
        '''
        print('Waiting for Tac3D sensor...')
        self.left_recvFlag = False
        self.right_recvFlag = False
        while not (self.left_recvFlag and self.right_recvFlag):
            if not self.left_recvFlag:
                print("left not connected")
            if not self.right_recvFlag:
                print("right not connected")
            time.sleep(0.1)
        print('Both Tac3D sensors connected.')


    def calibrate(self, SN):
        '''
        向Tac3D-Desktop发送一次传感器校准信号，要求设置当前的3D变形状态为
        无变形是的状态以消除“温漂”或“光飘”的影响。相当于在Tac3D-Desktop
        端点击一次 “零位校准”按键的效果。
        '''
        addr = self._fromAddrMap.get(SN)
        if addr != None:
            print('Calibrate signal send to %s.' % SN)
            self._UDP.send(b'$C', addr)
        else:
            print("Calibtation failed! (sensor %s is not connected)" % SN)
            
    def quitSensor(self, SN):
        '''
        向Tac3D-Desktop发送一次结束传感器线程的信号。相当于在Tac3D-Desktop
        端点击一次 “关闭传感器”按键的效果。
        '''
        addr = self._fromAddrMap.get(SN)
        if addr != None:
            print('Quit signal send to %s.' % SN)
            self._UDP.send(b'$Q', addr)
        else:
            print("Quit failed! (sensor %s is not connected)" % SN)

if __name__ == '__main__':
    SN = ''
    idx = -1
    sendTimestamp = 0.0
    recvTimestamp = 0.0

    P, D, F, Fr, Mr = None, None, None, None, None
    

    # 创建Sensor实例，设置回调函数为上面写好的Tac3DRecvCallback，设置UDP接收端口为9988
    tac3d = Sensor_ros1(port=9988)

    # 等待Tac3D-Desktop端启动传感器并建立连接
    tac3d.waitForFrame()
    
    time.sleep(1) # 5s

    # 发送一次校准信号（应确保校准时传感器未与任何物体接触！否则会输出错误的数据！）
    if tac3d.left_SN is not None:
        tac3d.calibrate(tac3d.left_SN)
    if tac3d.right_SN is not None:
        tac3d.calibrate(tac3d.right_SN)
        time.sleep(3)
    tac3d.ros_pub_start()
    
    print("The process will last for 10 sec")
    time.sleep(10)
    print("Done")
    

   

    # 获取frame的另一种方式：通过getFrame获取缓存队列中的frame
    #frame = tac3d.getFrame()
    #if not frame is None:
    #    print(frame['SN'])

    #time.sleep(5) #5s

    # # 发送一次关闭传感器的信号（不建议使用）
    # tac3d.quitSensor(SN)





