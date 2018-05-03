package com.iota.iri.model;

import javax.persistence.Table;
import java.util.List;


/**
 * Created by paul on 5/15/17.
 */
@Table(name = "t_address")
public class Address extends Hashes {
    private Long balance = 0L;

    private Long barsfBalance = 0L;

    public Long getBalance() {
        return balance;
    }

    public void setBalance(Long balance) {
        this.balance = balance;
    }

    public Long getBarsfBalance() {
        return barsfBalance;
    }

    public void setBarsfBalance(Long barsfBalance) {
        this.barsfBalance = barsfBalance;
    }

    public Address() {
    }

    public Address(Hash hash) {
        set.add(hash);
    }
    public Address(List<Hash> hashes) {
        set.addAll(hashes);
    }
}
