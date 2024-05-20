package org.mccproxy.ml;

import org.mccproxy.cache.AccessTracker;

import java.util.List;

public class ObsoleteItemsPredictor {
    private static ObsoleteItemsPredictor instance = null;

    FeatureExtractor featureExtractor = new FeatureExtractor();

    public ObsoleteItemsPredictor() {
    }

    public static ObsoleteItemsPredictor getInstance() {
        if (instance == null) {
            instance = new ObsoleteItemsPredictor();
        }
        return instance;
    }

    public List<Boolean> predictObsoleteItems(
            List<AccessTracker> accessTrackers) {
        return null;
    }
}
