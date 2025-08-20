Readme
======

Kmeans clustering algorithm based on map-reduce framework


1. Compile the proto files
python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. master_reducer.proto
python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. master_mapper.proto
python3 -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. reducer_mapper.proto

2. Create a directory with structure
    Data/
    ├─ Input/
    │  ├─ points.txt (initial points)
    ├─ Mappers/
    │  ├─ M1/
    │  │  ├─ partition_1.txt
    │  │  ├─ partition_2.txt
    ...
    │  │  ├─ partition_R.txt (R based on the number of reducers)
    │  ├─ M2/ ...
    │  ├─ M3/ ...
    ...
    ├─ Reducers/
    │  ├─ R1.txt
    │  ├─ R2.txt
    ├─ centroids.txt (final list of centroids)

3. Run multiple instances of mapper.py
4. Run multiple instances of reducer.py
5. Run a single instance of master.py
