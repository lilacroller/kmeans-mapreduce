import grpc
import random
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import point_pb2
import master_mapper_pb2
import master_mapper_pb2_grpc
import master_reducer_pb2
import master_reducer_pb2_grpc

mapperStarterPort= 50051
reducerStarterPort= 60051
splitPrefix= "./Data/Input/inputSplit"
centroidFilename= "./centroids.txt"
inputFile= "./Data/Input/points.txt"
randomRangeMinX= 20
randomRangeMinY= 20
randomRangeMaxX= 20
randomRangeMaxY= 20
delta= 1

class GRPCClient:
    def __init__(self):
        self._channels= {}
        self._stubs= {}

    def get_channel(self, port):
        address= f"localhost:{port}"
        if address not in self._channels:
            self._channels[address]= grpc.insecure_channel(address)
        return self._channels[address]

    def get_stub(self, port, method):
        address= f"localhost:{port}"
        if address not in self._stubs:
            channel = self.get_channel(port)
            self._stubs[address] = method(channel)
        return self._stubs[address]

    def close_all(self):
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()
        self._stubs.clear()

def splitInput(mapperCount):
    directory= "./Data/Input/"
    for f in os.listdir(directory):
        if os.path.isfile(f"{directory}/{f}") and f!="points.txt":
            os.remove(f"{directory}/{f}")



    if os.path.exists("./Data/Input/"):
        os.remove(centroidFilename)

    with open(inputFile,"r") as data:
        pointList= data.readlines()
        lineCount= len(pointList)
        data.seek(0)
        j= 0
        for i in range(mapperCount):
            filename= splitPrefix + str(i) + ".txt"
            file = open(filename, "w")
            for _ in range(lineCount//mapperCount):
                file.write(pointList[j])
                j+=1

            if i == mapperCount-1:
                while j < lineCount:
                    file.write(pointList[j])
                    j+=1
            file.close()



def generateCentroids(centroidCount, Xhigh, Xlow,
                      Yhigh, Ylow):
    centroidList= []
    for i in range(centroidCount):
        centroidList.append([random.uniform(Xlow, Xhigh),
                             random.uniform(Ylow,Yhigh)])

    return centroidList

def writeCentroidtoFile(centroids):
    if os.path.exists(centroidFilename):
        os.remove(centroidFilename)
    with open(centroidFilename, "a") as file:
        for point in centroids:
            file.write(f"{point[0]},{point[1]}\n")


def runMappers(mapperCount, reducerCount, centroidList):
    mapperObject = GRPCClient()
    ports = [port for port in range(mapperStarterPort, mapperStarterPort + mapperCount)]
    pool= ThreadPoolExecutor(max_workers=5)
    for i in range(mapperCount):
        pool.submit(runEachMapper, mapperObject, ports, reducerCount, centroidList, i)
    pool.shutdown(wait=True, cancel_futures=False)
    return
    
    
    
def runEachMapper(mapperObject, ports, reducerCount, centroidList, mapperID):
    stub= mapperObject.get_stub(ports[mapperID], master_mapper_pb2_grpc.InstructMapperStub)
    mapperInput= master_mapper_pb2.MapperInput()
    mapperInput.reducerCount= reducerCount
    mapperInput.datafile= f"{splitPrefix}{mapperID}.txt"
    for centroid in centroidList:
        point= point_pb2.Point()
        point.X= float(centroid[0])
        point.Y= float(centroid[1])
        mapperInput.centroids.append(point)
    stub.SendMapperInput(mapperInput)


def runReducers(reducerCount, mapperCount, centroidCount):
    reducerObject = GRPCClient()
    newCentroidList = []
    for i in range(centroidCount):
        newCentroidList.append([])
    ports = [port for port in range(reducerStarterPort, reducerStarterPort + reducerCount)]
    pool= ThreadPoolExecutor(max_workers=5)
    for i in range(reducerCount):
        pool.submit(runEachReducer, reducerObject, ports, reducerCount, mapperCount,
                     newCentroidList, i)
    pool.shutdown(wait= True, cancel_futures= False)
    return newCentroidList

def runEachReducer(reducerObject, ports, reducerCount, mapperCount, newCentroidList, reducerID):
    stub = reducerObject.get_stub(ports[reducerID], master_reducer_pb2_grpc.SetupReducerStub)
    index= master_reducer_pb2.IDs()
    index.reducerID= reducerID % reducerCount
    index.mapperCount= mapperCount
    centroidkv= stub.GetNewCentroids(index)
    for centroid in centroidkv.centroids:
        newCentroidList[centroid.centroidid]+= [centroid.value.X,centroid.value.Y]

    return


def fillVoid(centroidList):
    for i in range(len(centroidList)):
        if centroidList[i]==[]:
            centroidList[i]= [random.uniform(-randomRangeMinX,randomRangeMinY),
                              random.uniform(-randomRangeMaxX,randomRangeMaxY)]
    
    return centroidList
            

def getInputRange():
    global randomRangeMaxY
    global randomRangeMaxX
    global randomRangeMinX
    global randomRangeMinY
    maxX= float('-inf')
    minX= float('inf')
    maxY= float('-inf')
    minY= float('inf')
    with open(inputFile, "r") as file:
        point= file.readline()
        while point!="":
            points= point.split(",")
            pointX= float(points[0])
            pointY= float(points[1])
            if pointX>maxX:
                maxX= pointX
            if pointX<minX:
                minX= pointX
            if pointY>maxY:
                maxY= pointY
            if pointY<minY:
                minY= pointY
            point= file.readline()
    
    randomRangeMinX= minX
    randomRangeMinY= minY
    randomRangeMaxX= maxX
    randomRangeMaxY= maxY
    return

def getDistance(pointA, pointB):
    Xa= pointA[0]
    Ya= pointA[1]
    Xb= pointB[0]
    Yb= pointB[1]

    t1= pow(Xb-Xa,2)
    t2= pow(Yb-Ya,2)
    return pow(t1+t2,1/2)

def f(centroids):
    aggregate= 0
    with open(inputFile, "r") as file:
        line= file.readline()
        while line!="":
            pointPairstr= line.split(",")
            pointPair= []
            pointPair.append(float(pointPairstr[0]))
            pointPair.append(float(pointPairstr[1]))
            minDistance= float('inf')
            for centroid in centroids:
                distance= getDistance(pointPair, centroid)
                if distance<minDistance:
                    minDistance= distance
            aggregate+= pow(minDistance, 2)
            line= file.readline()
    
    return aggregate



def main():
    M= int(input("Enter mapper count: "))
    R= int(input("Enter reducer count: "))
    K= int(input("Enter no. of centroids: "))
    iterations= int(input("Enter no. of iterations for k-means: "))

    splitInput(M)
    getInputRange()
    logging.info(f"x range: {randomRangeMinX}-{randomRangeMaxX},"
                 f"y range: {randomRangeMinY}-{randomRangeMaxY}")
    newCentroids= generateCentroids(K, randomRangeMaxX, -randomRangeMaxY, 
                                    randomRangeMinX, -randomRangeMinY)
    writeCentroidtoFile(newCentroids)
    for i in range(iterations):
        runMappers(M, R, newCentroids)
        oldCentroids= newCentroids
        newCentroids= runReducers(R, M, K)
        fillVoid(newCentroids)
        writeCentroidtoFile(newCentroids)
        logging.info(f"centroids for iteration {i} are:")
        for centroid in newCentroids:
            logging.info(f"{centroid}")
        if abs(f(oldCentroids) - f(newCentroids)) < delta:
            break

    for centroid in newCentroids:
        print(f"{centroid[0]}, {centroid[1]}")
    

    return
if __name__ == "__main__":
    logging.basicConfig(
        filename="master.log",
        encoding="utf-8",
        filemode="a",
        format= "{asctime} - {levelname} - {message}",
        style= "{",
        datefmt= "%Y-%m-%d %H:%M",
        level=logging.DEBUG,
    )
    main()
