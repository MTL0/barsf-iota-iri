package com.iota.iri.model;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class Bundle extends Hashes {

    public Bundle(Hash hash) {
        set.add(hash);
    }

    public Bundle(List<Hash> hash) {
        set.addAll(hash);
    }
    public Bundle() {

    }
}
