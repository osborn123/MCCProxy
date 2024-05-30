package org.mccproxy.ml;

import org.mccproxy.cache.AccessTracker;

import java.util.List;

public class SimpleObsoleteItemsPredictor extends ObsoleteItemsPredictor {

    public SimpleObsoleteItemsPredictor() {
        super();
    }

    @Override
    public List<Boolean> predictObsoleteItems(
            List<AccessTracker> accessTrackers) {
        return List.of();
    }
}
