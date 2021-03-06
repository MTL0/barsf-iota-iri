package com.iota.iri.storage.innoDB.mybatis.subProvider;

import com.iota.iri.model.Address;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.innoDB.NeededException;
import com.iota.iri.storage.innoDB.mybatis.DbHelper;
import com.iota.iri.storage.innoDB.mybatis.modelMapper.AddressMapper;
import com.iota.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.iota.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;

/**
 * Created by ZhuDH on 2018/3/30.
 * <p>
 */
public class AddressProvider implements SubPersistenceProvider {
    private static Logger log = LoggerFactory.getLogger(AddressProvider.class);

    private static volatile AddressProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private AddressProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static AddressProvider getInstance() {
        synchronized (AddressProvider.class) {
            if (instance == null) {
                instance = new AddressProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        // 地址处理使用
        if (outSession != null) {
            AddressMapper addressMapper = outSession.getMapper(AddressMapper.class);
            ((Address) thing).setHash(converterIndexableToStr(index));
            return addressMapper.insertOrUpdate((Address) thing) > 0;
        } else {
            return false;
        }
    }


    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws Exception {
        // 未曾使用
        throw new NeededException();
    }

    public boolean save(Persistable thing, Indexable index) {
        throw new NeededException();
    }

    @Override
    public void delete(Indexable index) throws Exception {
    }

    @Override
    public boolean update(Persistable model, Indexable index, String item) throws Exception {
        // 当前版本没有对Approvee的直接更新
        throw new NeededException();
    }

    @Override
    public boolean exists(Indexable key) throws Exception {
        // 只有trans/milestone使用
        throw new NeededException();
    }

    public boolean exists(Indexable key, SqlSession session) throws Exception {
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        // 只有里程碑使用
        throw new NeededException();
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        return null;
    }

    @Override
    public Persistable get(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            List<String> hashOfTransByTag = mapper.selectHashesByAddress(converterIndexableToStr(index));
            if (hashOfTransByTag.size() > 0) {
                return new Address(hashOfTransByTag.stream().map(Hash::new).collect(Collectors.toList()));
            } else {
                return Address.class.newInstance();
            }
        }
    }

    @Override
    public boolean mayExist(Indexable index) throws Exception {
        return exists(index);
    }

    @Override
    public long count() throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    @Override
    public Set<Indexable> keysStartingWith(byte[] value) {
        throw new NeededException();
    }

    @Override
    public Persistable seek(byte[] key) throws Exception {
        // 只有trans使用
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> next(Indexable index) throws Exception {
        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }

    @Override
    public Pair<Indexable, Persistable> previous(Indexable index) throws Exception {
        // 只有milestone使用
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> first(Class<?> indexModel) throws Exception {
        // rescrn操作使用, 实际什么都不用做
        return new Pair<>(null, null);
    }

    public int updateByPrimaryKeySelective(Address add, SqlSession outerSession) {
        if (outerSession != null) {
            AddressMapper mapper = outerSession.getMapper(AddressMapper.class);
            return mapper.updateByPrimaryKeySelective(add);
        }

        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            AddressMapper mapper = session.getMapper(AddressMapper.class);
            return mapper.updateByPrimaryKeySelective(add);
        }
    }

    public void saveUnUpdate(Persistable thing, Indexable index, SqlSession session) {
        AddressMapper mapper = session.getMapper(AddressMapper.class);
        ((Address) thing).setHash(converterIndexableToStr(index));
        mapper.insert((Address) thing);
    }
}
