import os
import threading
import grpc
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED 
from concurrent import futures

import reducer_mapper_pb2
import reducer_mapper_pb2_grpc
import master_reducer_pb2
import master_reducer_pb2_grpc

reducerID= 0
reducerFilePrefix="./Data/Reducers/R"
reducerPort= 60051
activeMapperList= []
initActiveMappers= False

class SetupReducerServicer(master_reducer_pb2_grpc.SetupReducerServicer):
    def GetNewCentroids(self, request, context):
        logging.info(f"reducer{reducerID}: getNewCentroids gets called")
        KVdict= shuffleandsort(request.reducerID, request.mapperCount)
        logging.info(f"redcuer{reducerID}: received Key Value list")
        response= master_reducer_pb2.CentroidList()
        if os.path.exists(f"{reducerFilePrefix}{reducerID}.txt"):
            os.remove(f"{reducerFilePrefix}{reducerID}.txt")
            logging.info(f"reducer{reducerID}: deleted {reducerFilePrefix}{reducerID}.txt file")
        with open(f"{reducerFilePrefix}{reducerID}.txt", "w") as file:
            for k,v in KVdict.items():
                sum0= 0
                sum1= 0
                for point in v:
                    sum0+= point[0]
                    sum1+= point[1]
                newCentroid= [sum0/len(v), sum1/len(v)]
                logging.info(f"reducer{reducerID}: calculated the new centroid")
                file.write(f"{k}:{newCentroid[0]},{newCentroid[1]}\n")
                logging.info(f"reducer{reducerID}: wrote the new centroid to file")
                responseCentroid= master_reducer_pb2.NewCentroid()
                responseCentroid.centroidid= k
                responseCentroid.value.X= newCentroid[0]
                responseCentroid.value.Y= newCentroid[1]
                response.centroids.append(responseCentroid)
        
        response.status= 100
        return response

def initializeActiveMapper(mapperCount):
    global initActiveMappers
    global activeMapperList
    ports= []
    for i in range(50051, 50051+mapperCount):
        ports.append(i)
    initActiveMappers= True
    activeMapperList= ports
    return ports

def shuffleandsort(redIDbyMaster, mapperCount):
    if initActiveMappers==False:
        initializeActiveMapper(mapperCount)

    tags= {}
    logging.info(f"reducer{reducerID}: starting threadpool...")
    
    pool= ThreadPoolExecutor(max_workers=5)
    futures= set()
    channels= {}
    stubs= {}
    print(f"length of activeMapperList: {len(activeMapperList)}")
    for i in range(len(activeMapperList)):
        channel = grpc.insecure_channel(f"localhost:{activeMapperList[i]}")
        channels[activeMapperList[i]]= channel
        stub= reducer_mapper_pb2_grpc.KVStub(channel)
        stubs[activeMapperList[i]]= stub
        future= pool.submit(getKeyValueList,redIDbyMaster, stub)
        futures.add(future)
        tags[future]= [activeMapperList[i], i]
    KVdict= {}
    logging.info(f"reducer{reducerID}: threadpool called, collecting results...")



    print(f"futures length:{len(futures)}")
    while futures:
        done, not_done= wait(futures, timeout=5, return_when=ALL_COMPLETED)

        for finished in done:
            try:
                result= finished.result()
            except grpc._channel._InactiveRpcError:
                print(f"port {tags[finished][0]} unaivalable")
                activeMapperList.remove(tags[finished][0])
                channels[tags[finished][0]].close()
                futures.remove(finished)
                port= random.choice(activeMapperList)
                future= pool.submit(getKeyValueList, redIDbyMaster, stubs[port])
                futures.add(future)
                tags[future]= [port, tags[finished][1]]
                del channels[tags[finished][0]]
                del stubs[tags[finished][0]]
                del tags[finished]
                continue
            
            print(f"result.status: {result.status}")
            if result.status==100:
                logging.info(f"reducer{reducerID}: mapper returned 100")
                print(f"mapper returned status 100")
                valuePoints= {}
                for point in result.kv:
                    if valuePoints.get(point.key)==None:
                        valuePoints[point.key]= []
                    valuePoints[point.key].append([point.value.X, point.value.Y])
                print(f"valuePoints: {valuePoints}")
                for k,v in valuePoints.items():
                    if k not in KVdict:
                        KVdict[k]= []
                    KVdict[k]= KVdict[k]+v

                futures.remove(finished)
            else:
                futures.remove(finished)
                port= random.choice(activeMapperList)
                future= pool.submit(getKeyValueList, redIDbyMaster, stubs[port])
                futures.add(future)
    
    for port in activeMapperList:
        channels[port].close()
        del channels[port]
        del stubs[port]



    
    logging.info(f"reducer{reducerID}: shutting down threadpool")
    pool.shutdown(wait=True, cancel_futures=False)	
    logging.info(f"reducer{reducerID}: threadpool shutted down, leaving shuffleandsort")
    print(KVdict)
    return KVdict

        
        


def getKeyValueList(id, stub):
#    channel = grpc.insecure_channel(f"localhost:{50051+mapperID}")
#    stub= reducer_mapper_pb2_grpc.KVStub(channel)
    index= reducer_mapper_pb2.Index()
    index.centroidid= id
    logging.info(f"requesting kv data from mapper for id {index.centroidid}...")
    kv= stub.getKV(index)
    logging.info(f"received kv data from mapper")   
    return kv

def serve(port):
    server = grpc.server(futures.ThreadPoolExecutor())
    master_reducer_pb2_grpc.add_SetupReducerServicer_to_server(SetupReducerServicer(), server)
    server.add_insecure_port(f"[::]:{port}")
    x= threading.Thread(target=server.start)
    logging.info(f"reducer{reducerID}: starting reducer server")
    x.start()
    server.wait_for_termination()

def main():
    global reducerID
    reducerID= int(input("Enter reducerID: "))
    serve(reducerPort+reducerID)

if __name__ == "__main__":
    logging.basicConfig(
        filename="reducer.log",
        encoding="utf-8",
        filemode="a",
        format= "{asctime} - {levelname} - {message}",
        style= "{",
        datefmt= "%Y-%m-%d %H:%M",
        level=logging.DEBUG,
    )
    main()
