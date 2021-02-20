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

    public ArrayList<Object> valuesFor(Serie serie) {
        return this.valuesFor(serie, null, null);
    }

    public ArrayList<Object> valuesFor(Serie serie, Double from, Double to) {
        ArrayList<Object> values = null;
        Memtable memtable = memtables.get(serie);
        if (memtable != null) {
            if (from != null && to != null) {
                values = new ArrayList(memtable.subMapValues(from, to));
            } else {
                values = new ArrayList(memtable.values());
            }
        }
        return values;
    }

    public void remove(Serie serie) {
        memtables.remove(serie);
    }

    // we consider that add is done in a single thread
    public void add(double time, Serie serie, Object event) {
        Memtable memtable = memtables.get(serie);
        if (memtable == null) {
            // create a new memtable if needed
            memtable = new Memtable(time, event, serie);
            memtables.put(serie, memtable);
        } else {
            double startTime = memtable.startTime();
            if (time > (startTime + memtableMaxTTL)) {
                // clear expired values from the memtable
                double newStartTime = startTime + memtableCleanupDuration;
                memtable.headMapClear(newStartTime);
                memtable.add(time, event);
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
