package fr.mcorbin.mirabelle.memtable;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReentrantLock;

public class Memtable {
    private ConcurrentSkipListMap<Double, Object> memtable;
    private Serie serie;
    private ReentrantLock lock;

    public Memtable(double firstTime, Object firstEvent, Serie serie) {
        this.memtable = new ConcurrentSkipListMap<Double, Object>();
        this.serie = serie;
        this.memtable.put(firstTime, firstEvent);
        this.lock = new ReentrantLock();
    }

    public void add(double time, Object event) {
        this.memtable.put(time, event);
    }

    public NavigableMap<Double, Object> subMap(double from, double to) {
        return memtable.subMap(from, true, to, true);
    }

    public Collection<Object> subMapValues(double from, double to) {
        return this.subMap(from, to).values();
    }

    public Collection<Object> values() {
        return memtable.values();
    }

    public void headMapClear(double to) {
        memtable.headMap(to).clear();
    }

    public double startTime() {
        return memtable.firstKey();
    }

    public double endTime() {
        return memtable.lastKey();
    }

    public ReentrantLock getLock() {
        return this.lock;
    }

    public void clear() {
        memtable.clear();
    }

    public void putAll(Map<Double, Object> map) {
        memtable.putAll(map);
    }
}
