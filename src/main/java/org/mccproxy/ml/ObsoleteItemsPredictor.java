package org.mccproxy.ml;

import org.mccproxy.cache.AccessTracker;

import java.util.List;

public abstract class ObsoleteItemsPredictor {

    protected ObsoleteItemsPredictor() {
    }

    abstract public List<Boolean> predictObsoleteItems(
            List<AccessTracker> accessTrackers);
}
