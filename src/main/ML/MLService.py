from concurrent import futures
import grpc
import pandas as pd

import MLService_pb2 as MLService
import MLService_pb2_grpc as MLService_grpc
from FeatureExtractor import extract_features

class MLServiceImpl(MLService_grpc.MLServiceServicer):
    def Predict(self, request, context):
        # Extract features from the request
        features = request.features
        # convert features to pandas dataframe
        # read accesses is the first colomn
        # write accesses is the second column
        raw_features = pd.DataFrame(features)
        extracted_features = extract_features(raw_features)
        # Process each feature and determine if it's obsolete
        is_obsolete_list = []
        # Return a PredictResponse with the is_obsolete list
        return MLService.PredictResponse(is_obsolete=is_obsolete_list)

    def is_feature_obsolete(self, new_feature):
        # Determine if the new feature is obsolete
        return new_feature > 0.5


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    MLService_grpc.add_MLServiceServicer_to_server(MLServiceImpl(), server)
    server.add_insecure_port('[::]:50051')
    print("Starting server. Listening on port 50051")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()