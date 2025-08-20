import os
import threading
import grpc
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent import futures

import reducer_mapper_pb2
import reducer_mapper_pb2_grpc
import master_reducer_pb2
import master_reducer_pb2_grpc

reducerID= 0
reducerFilePrefix="./Data/Reducers/R"
reducerPort= 60051

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
        
        return response



def shuffleandsort(redIDbyMaster, mapperCount):
    logging.info(f"reducer{reducerID}: starting threadpool...")
    pool= ThreadPoolExecutor(max_workers=5)
    futures= [pool.submit(getKeyValueList,redIDbyMaster, mapperID)
                for mapperID in range(mapperCount)]
    KVdict= {}
    logging.info(f"reducer{reducerID}: threadpool called, collecting results...")
    for fut in as_completed(futures):
        resultDict= fut.result()
        for k,v in resultDict.items():
            if k not in KVdict:
                KVdict[k]= []
            KVdict[k]= KVdict[k]+v
    
    logging.info(f"reducer{reducerID}: shutting down threadpool")
    pool.shutdown(wait=True, cancel_futures=False)	
    logging.info(f"reducer{reducerID}: threadpool shutted down, leaving shuffleandsort")
    return KVdict
    
        
        


def getKeyValueList(id, mapperID):
    channel = grpc.insecure_channel(f"localhost:{50051+mapperID}")
    stub= reducer_mapper_pb2_grpc.KVStub(channel)
    index= reducer_mapper_pb2.Index()
    index.centroidid= id
    logging.info(f"requesting kv data from mapper for id {index.centroidid}...")
    x= stub.getKV(index)
    channel.close()
    logging.info(f"received kv data from mapper")
    valuePoints= {}
    for point in x.kv:
        if valuePoints.get(point.key)==None:
            valuePoints[point.key]= []
        valuePoints[point.key].append([point.value.X, point.value.Y])
    
    return valuePoints

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
