package com.iota.iri.model;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class Tag extends Hashes {
    public Tag(Hash hash) {
        set.add(hash);
    }

    public Tag(List<Hash> hashes){
        set.addAll(hashes);
    }

    public Tag() {

    }
}
