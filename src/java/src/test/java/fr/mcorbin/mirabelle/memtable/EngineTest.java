package fr.mcorbin.mirabelle.memtable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

public class EngineTest {

    @Test
    @DisplayName("Adding values to the engine, trigger several cleanups")
    public void testAddCleanup() {
        Engine engine = new Engine(60, 30);;
        HashMap<String, String> labels = new HashMap<>();
        labels.put("host", "mcorbin.fr");
        Serie serie = new Serie("cpu", labels);
        engine.add(1, serie, 1);
        assert (engine.valuesFor(serie).containsAll(Arrays.asList(1)));
        assertEquals(1, engine.valuesFor(serie).size());

        engine.add(59, serie, 2);
        assert (engine.valuesFor(serie).containsAll(Arrays.asList(2)));
        assertEquals(2, engine.valuesFor(serie).size());

        engine.add(62, serie, 3);
        assert (engine.valuesFor(serie).containsAll(Arrays.asList(2, 3)));
        assertEquals(2, engine.valuesFor(serie).size());

        engine.add(62, serie, 4);
        assertEquals(2, engine.valuesFor(serie).size());
        assert (engine.valuesFor(serie).containsAll(Arrays.asList(2, 4)));
        engine.add(70, serie, 5);
        engine.add(95, serie, 6);
        engine.add(96, serie, 7);
        engine.add(121, serie, 8);

        Collection<Object> values = engine.valuesFor(serie);
        assertEquals(3, values.size());
        assert (engine.valuesFor(serie).containsAll(Arrays.asList(6, 7, 8)));

        engine.add(300, serie, 7);
        assertEquals(3, values.size());
        assertEquals(1, engine.valuesFor(serie).size());
    }
}
