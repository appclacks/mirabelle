package fr.mcorbin.mirabelle.memtable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Engine {
    private int memtableMaxTTL;
    private int memtableCleanupDuration;
    private ConcurrentHashMap<Serie, Memtable> memtables;

    public Engine(int memtableMaxTTL, int memtableCleanupDuration) {
        this.memtableMaxTTL = memtableMaxTTL;
        this.memtableCleanupDuration = memtableCleanupDuration;
        this.memtables = new ConcurrentHashMap<>();
    }

    public Collection<Object> valuesFor(Serie serie) {
        return this.valuesFor(serie, null, null);
    }

    public Collection<Object> valuesFor(Serie serie, Double from, Double to) {
        Collection<Object> values = null;
        Memtable memtable = memtables.get(serie);
        if (memtable != null) {
            if (from != null && to != null) {
                values = memtable.subMapValues(from, to);
            } else {
                values = memtable.values();
            }
        }
        return values;
    }

    public void remove(Serie serie) {
        memtables.remove(serie);
    }

    public void add(double time, Serie serie, Object event) {
        Memtable memtable = memtables.get(serie);
        if (memtable == null) {
            // create a new memtable if needed
            // use compute to be safe
            memtables.compute(serie, (k, v) -> {
                if (v == null) {
                    // create the new memtable if null
                    return new Memtable(time, event, serie);
                }
                // else, add the event to the current memtable
                v.add(time, event);
                return v;
            });
        } else {
            double startTime = memtable.startTime();
            // first check if the values should be purged, then try to ascquire the lock
            // if the lock is not acquired, do not try to clear (someone else may be
            // doing it
            if (time > (startTime + memtableMaxTTL) && memtable.getLock().tryLock()) {
                try {
                    // clear expired values from the memtable
                    double newStartTime = startTime + memtableCleanupDuration;
                    memtable.headMapClear(newStartTime);
                    memtable.add(time, event);
                } finally {
                    memtable.getLock().unlock();
                }
            } else {
                // just add the value
                memtable.add(time, event);
            }
        }
    }

    public ArrayList<Serie> getSeries() {
        return new ArrayList(this.memtables.keySet());
    }
}
