package com.iota.iri.model;

import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.innoDB.mybatis.DbHelper;
import com.iota.iri.utils.Serializer;
import org.apache.commons.lang3.ArrayUtils;

import javax.persistence.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by paul on 5/6/17.
 */
@Table(name = "t_state_diff")
public class StateDiff implements Persistable {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY,generator="Mysql")
    private String hash;
    @Column(name = "trytes")
    private byte[] trytes;

    public String getHash() {
        return hash;
    }

    public byte[] getTrytes() {
        return trytes;
    }

    public void setTrytes(byte[] trytes) {
        this.trytes = trytes;
        read(trytes);
    }

    public void filling(Indexable index) {
        this.trytes = bytes();
        this.hash = DbHelper.converterIndexableToStr(index);
    }

    public void filling(Indexable index, byte[] bytes) {
        this.trytes = bytes;
        this.hash = DbHelper.converterIndexableToStr(index);
    }

    @Transient
    public Map<Hash, Long> state;

    public byte[] bytes() {
        return state.entrySet().parallelStream()
                .map(entry -> ArrayUtils.addAll(entry.getKey().bytes(), Serializer.serialize(entry.getValue())))
                .reduce(ArrayUtils::addAll)
                .orElse(new byte[0]);
    }
    public void read(byte[] bytes) {
        int i;
        state = new HashMap<>();
        if(bytes != null) {
            for (i = 0; i < bytes.length; i += Hash.SIZE_IN_BYTES + Long.BYTES) {
                state.put(new Hash(bytes, i, Hash.SIZE_IN_BYTES),
                        Serializer.getLong(Arrays.copyOfRange(bytes, i + Hash.SIZE_IN_BYTES, i + Hash.SIZE_IN_BYTES + Long.BYTES)));
            }
        }
    }

    @Override
    public byte[] metadata() {
        return new byte[0];
    }

    @Override
    public void readMetadata(byte[] bytes) {
    }

    @Override
    public boolean merge() {
        return false;
    }

}
