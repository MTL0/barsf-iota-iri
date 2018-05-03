package com.iota.iri.model;

import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Serializer;
import org.apache.commons.lang3.ArrayUtils;

import javax.persistence.*;

/**
 * Created by paul on 4/11/17.
 */
@Table(name = "t_milestone")
public class Milestone implements Persistable {
    @Id
    @Column(name = "mt_index")
    @GeneratedValue(strategy= GenerationType.IDENTITY,generator="Mysql")
    public IntegerIndex index;

    @Column(name = "tx_hash")
    public Hash hash;

    public Integer getIndex() {
        return index.value;
    }

    public void setIndex(Integer index) {
        this.index = new IntegerIndex(index);
    }

    public String getHash() {
        return hash.toString();
    }

    public void setHash(String hash) {
        this.hash = new Hash(hash);
    }

    public byte[] bytes() {
        return ArrayUtils.addAll(index.bytes(), hash.bytes());
    }

    public void read(byte[] bytes) {
        if(bytes != null) {
            index = new IntegerIndex(Serializer.getInteger(bytes));
            hash = new Hash(bytes, Integer.BYTES, Hash.SIZE_IN_BYTES);
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
