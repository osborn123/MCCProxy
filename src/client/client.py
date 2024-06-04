import grpc
from src.main.ML import mccproxy_pb2
from src.main.ML import mccproxy_pb2_grpc

def run():

    channel = grpc.insecure_channel('localhost:8980')
    stub = mccproxy_pb2_grpc.MCCProxyServiceStub(channel)
    
    
    read_request = mccproxy_pb2.ReadRequest(keys=[
        'user1573987489603120213',
        'user4862176667600626343',
        'user4052466453699787802',
        'user296624322641201295',
        'user1116048545874873763',
        'user3599432007208650916',
        'user4519777761466569614',
        'user8285893862370130496'
    ])
    
   
    response = stub.Read(read_request)
    
    
    for item in response.items:
        print(f"Key: {item.key}, Value: {item.value}")

if __name__ == '__main__':
    run()