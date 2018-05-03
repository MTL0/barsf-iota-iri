package com.iota.iri.model;

import com.iota.iri.storage.Persistable;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Created by paul on 3/8/17 for iri.
 */

public class Hashes implements Persistable {
    private static final Logger log = LoggerFactory.getLogger(Hashes.class);
    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "Mysql")
    private String hash;

    public String getHash() {
        return hash;
    }

    public void setHash(String hash) {
        this.hash = hash;
    }

    @Transient
    public Set<Hash> set = new LinkedHashSet<>();
    @Transient
    private static final byte delimiter = ",".getBytes()[0];

    public byte[] bytes() {
        if (set.size() > 3000){
            log.info("this may be take a lots of time "+ hash);
            return new byte[0];
        }
        long start = System.currentTimeMillis();
        byte[] temp = set.parallelStream()
                .map(Hash::bytes)
                .reduce((a, b) -> ArrayUtils.addAll(ArrayUtils.add(a, delimiter), b))
                .orElse(new byte[0]);
        if (set.size()>300) {
            log.info(" map-reduce hashes to bytes cost :" + (System.currentTimeMillis() - start) + " size:" + temp.length);
        }

        return temp;
    }

    public void read(byte[] bytes) {
        if (bytes != null) {
            set = new LinkedHashSet<>(bytes.length / (1 + Hash.SIZE_IN_BYTES) + 1);
            for (int i = 0; i < bytes.length; i += 1 + Hash.SIZE_IN_BYTES) {
                set.add(new Hash(bytes, i, Hash.SIZE_IN_BYTES));
            }
        }
    }

    private static LinkedHashSet<Hash> reverse(LinkedHashSet<Hash> set) {
        if (set != null && set.size() > 0) {
            // convert to ArrayList
            ArrayList<Hash> alCompanies =
                    new ArrayList<>(set);

            // to reverse LinkedHashSet contents
            Collections.reverse(alCompanies);
            set.clear();
            set.addAll(alCompanies);
        }
       throw new RuntimeException();
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
        return true;
    }
}
