package org.mccproxy.ml;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.mccproxy.cache.AccessTracker;

import java.util.List;

public class MLObsoleteItemsPredictor extends ObsoleteItemsPredictor {


    private MLServiceGrpc.MLServiceBlockingStub blockingStub;


    public MLObsoleteItemsPredictor(String host, int port) {
        super();
        ManagedChannel managedChannel =
                ManagedChannelBuilder.forAddress(host, port).usePlaintext()
                        .build();

        blockingStub = MLServiceGrpc.newBlockingStub(managedChannel);
    }

    @Override
    public List<Boolean> predictObsoleteItems(
            List<AccessTracker> accessTrackers) {
        PredictRequest request = PredictRequest.newBuilder().addAllFeatures(
                accessTrackers.stream().map(AccessTracker::toRawFeature)
                        .toList()).build();

        PredictResponse response = blockingStub.predict(request);
        return response.getIsObsoleteList();
    }
}
