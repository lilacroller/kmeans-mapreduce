from concurrent import futures
import grpc
import threading
import os
import logging

import master_mapper_pb2
import master_mapper_pb2_grpc
import reducer_mapper_pb2
import reducer_mapper_pb2_grpc


selfStarterPort= 50051
mapperID= 0
mapperFilePrefix= "./Data/Mappers/M"

def getDistance(pointA, pointB):
    Xa= pointA[0]
    Ya= pointA[1]
    Xb= pointB[0]
    Yb= pointB[1]

    t1= pow(Xb-Xa,2)
    t2= pow(Yb-Ya,2)
    return pow(t1+t2,1/2)

def deleteFilesInDir(directory):
    for f in os.listdir(directory):
        if os.path.isfile(f"{directory}/{f}"):
            os.remove(f"{directory}/{f}")
    return

def getCluster(pointList, centroidList):
    clusters = {}

    for point in pointList:
        smallestIndex= -1
        smallestDistance= float('inf')
        for i in range(len(centroidList)):
            candidate= pow(getDistance(point,centroidList[i]),2)
            if candidate < smallestDistance:
                smallestDistance= candidate
                smallestIndex= i
            
            if clusters.get(smallestIndex)==None:
                clusters[smallestIndex]= []
        clusters[smallestIndex].append(point)
            
    return clusters

def writeCluster2Files(clusters, reducerCount):
    partitionDir= f"{mapperFilePrefix}{mapperID}"
    if not os.path.exists(partitionDir):
        os.mkdir(partitionDir)
    deleteFilesInDir(partitionDir)
    partitionFileList= []
    for i in range(reducerCount):
        file = open(f"{partitionDir}/{i}.txt", "w")
        partitionFileList.append(file)   

    k= 0
    for centroid, pointList in clusters.items():
        index= k % reducerCount
        file= partitionFileList[index]
        for point in pointList:
            file.write(f"{index}:{point[0]},{point[1]}\n")
        k+=1

    for i in range(reducerCount):
        partitionFileList[i].close()

    return
        
class InstructMapperServicer(master_mapper_pb2_grpc.InstructMapperServicer):
    def SendMapperInput(self, request, context):
        logging.info(f"mapper{mapperID}: mapper received request for SendMapperInput")
        reducerCount= request.reducerCount
        inputFile= request.datafile
        centroids= request.centroids
        centroidList= []
        for centroid in centroids:
            centroidList.append([centroid.X,centroid.Y])

        file= open(inputFile,"r")
        pointList= []
        strpoint= file.readline()
        while strpoint!='':
            pointPair= strpoint.split(',')
            pointX= float(pointPair[0])
            pointY= float(pointPair[1])
            pointList.append([pointX, pointY])
            strpoint= file.readline()
        file.close()
        clusters= getCluster(pointList, centroidList)
        writeCluster2Files(clusters, reducerCount)
        logging.info(f"mapper{mapperID}: SendMapperInput processed and wrote the clusters in the files")


        status= master_mapper_pb2.Status()
        status.status_code= 100
        return status

class KVServicer(reducer_mapper_pb2_grpc.KVServicer):
    def getKV(self, request, context):
        logging.info(f"mapper{mapperID}: received request for getKV")
        response= reducer_mapper_pb2.KVdata()
        response_keyval= reducer_mapper_pb2.KeyValue()
        index= request.centroidid
        file = open(f"{mapperFilePrefix}{mapperID}/{index}.txt", "r")
        logging.info(f"mapper{mapperID}: opened key-value file")
        strkv= file.readline()
        while strkv!="":
            strkvlist= strkv.split(":")
            response_keyval.key= int(strkvlist[0])
            strvallist= strkvlist[1].split(",")
            response_keyval.value.X= float(strvallist[0])
            response_keyval.value.Y= float(strvallist[1])
            response.kv.append(response_keyval)
            strkv= file.readline()

        logging.info(f"mapper{mapperID}: compiled key value list")
        return response

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor())
    master_mapper_pb2_grpc.add_InstructMapperServicer_to_server(InstructMapperServicer(), server)
    reducer_mapper_pb2_grpc.add_KVServicer_to_server(KVServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    x= threading.Thread(target=server.start)
    x.start()
    server.wait_for_termination()

def main():
    global mapperID
    mapperID= int(input("Enter mapper id: "))
    port= selfStarterPort+mapperID
    logging.info(f"my mapperID is {mapperID}, my port is {port}")
    serve(port)

if __name__ == "__main__":
    logging.basicConfig(
        filename="mapper.log",
        encoding="utf-8",
        filemode="a",
        format= "{asctime} - {levelname} - {message}",
        style= "{",
        datefmt= "%Y-%m-%d %H:%M",
        level=logging.DEBUG,
    )
    port = main()


