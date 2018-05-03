package com.iota.iri.storage.rocksDB;

import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.PersistenceProvider;
import com.iota.iri.utils.Pair;

import java.util.List;
import java.util.Set;

/**
 * Created by ZhuDH on 2018/4/30.
 */
public class RocksDBPersistenceProvider implements PersistenceProvider {
    public RocksDBPersistenceProvider(String absolutePath, String absolutePath1, int i) {
    }

    @Override
    public void init() throws Exception {

    }

    @Override
    public boolean isAvailable() {
        return false;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public boolean save(Persistable model, Indexable index) throws Exception {
        return false;
    }

    @Override
    public void delete(Class<?> model, Indexable index) throws Exception {

    }

    @Override
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        return false;
    }

    @Override
    public boolean exists(Class<?> model, Indexable key) throws Exception {
        return false;
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> model, Class<?> indexModel) throws Exception {
        return null;
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> modelClass, Class<?> otherClass) throws Exception {
        return null;
    }

    @Override
    public Persistable get(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public boolean mayExist(Class<?> model, Indexable index) throws Exception {
        return false;
    }

    @Override
    public long count(Class<?> model) throws Exception {
        return 0;
    }

    @Override
    public Set<Indexable> keysStartingWith(Class<?> modelClass, byte[] value) {
        return null;
    }

    @Override
    public Persistable seek(Class<?> model, byte[] key) throws Exception {
        return null;
    }

    @Override
    public Pair<Indexable, Persistable> next(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public Pair<Indexable, Persistable> previous(Class<?> model, Indexable index) throws Exception {
        return null;
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> model, Class<?> indexModel) throws Exception {
        return null;
    }

    @Override
    public int saveBatch(List<Pair<Indexable, Persistable>> models) throws Exception {
        return 0;
    }

    @Override
    public void clear(Class<?> column) throws Exception {

    }

    @Override
    public void clearMetadata(Class<?> column) throws Exception {

    }
}
