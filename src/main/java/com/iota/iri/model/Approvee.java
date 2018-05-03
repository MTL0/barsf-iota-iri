package com.iota.iri.model;

import java.util.List;

/**
 * Created by paul on 5/15/17.
 */
public class Approvee extends Hashes {
    public Approvee(Hash hash) {
        set.add(hash);
    }

    public Approvee(List<Hash> hashes){
        set.addAll(hashes);
    }

    public Approvee() {

    }
}
