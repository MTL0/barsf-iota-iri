package com.iota.iri.storage.innoDB.mybatis.subProvider;

import com.iota.iri.model.Hash;
import com.iota.iri.model.Tag;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.innoDB.NeededException;
import com.iota.iri.storage.innoDB.mybatis.DbHelper;
import com.iota.iri.storage.innoDB.mybatis.modelMapper.TagMapper;
import com.iota.iri.storage.innoDB.mybatis.modelMapper.TransactionMapper;
import com.iota.iri.utils.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tk.mybatis.mapper.entity.Example;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.iota.iri.storage.innoDB.mybatis.DbHelper.converterIndexableToStr;

/**
 * Created by ZhuDH on 2018/3/30.
 */
public class TagProvider implements SubPersistenceProvider {
    private static Logger LOG = LoggerFactory.getLogger(TagProvider.class);

    private static volatile TagProvider instance = null;
    private final SqlSessionFactory sessionFactory;

    private TagProvider() {
        this.sessionFactory = DbHelper.getSingletonSessionFactory();
    }

    public static TagProvider getInstance() {
        synchronized (TagProvider.class) {
            if (instance == null) {
                instance = new TagProvider();
            }
        }
        return instance;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode, AtomicLong al) {
        // saveBatch使用(无意义), 不报错即为true
        return true;
    }

    public boolean save(Persistable thing, Indexable index, SqlSession outSession, boolean mergeMode) {
        // 使用数据库关系,  直接从trans中获取, 因此也不需要占用这个保存数据
        return save(thing, index, outSession, mergeMode, new AtomicLong());
    }

    public boolean save(byte[] thing, Indexable index, SqlSession outSession, boolean mergeMode) throws IllegalAccessException, InstantiationException {
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
        // 当前版本没有对tag的直接更新
        throw new NeededException();
    }

    @Override
    public boolean exists(Indexable key) throws Exception {
        // 只有trans/milestone使用
        throw new NeededException();
    }

    public boolean exists(Indexable key, SqlSession outerSession) throws Exception {
        throw new NeededException();
    }

    @Override
    public Pair<Indexable, Persistable> latest(Class<?> indexModel) throws Exception {
        // 只有里程碑使用
        throw new NeededException();
    }

    @Override
    public Set<Indexable> keysWithMissingReferences(Class<?> otherClass) throws Exception {
        // 至今未用
        throw new NeededException();
    }

    @Override
    public Persistable get(Indexable index) throws Exception {
        try (final SqlSession session = this.sessionFactory.openSession(true)) {
            TransactionMapper mapper = session.getMapper(TransactionMapper.class);
            Example selectHash = new Example(Transaction.class);
            selectHash.selectProperties("hash");
            selectHash.createCriteria().andEqualTo("tag", converterIndexableToStr(index));
            List<Transaction> hashOfTransByTag = mapper.selectByExample(selectHash);
            if (hashOfTransByTag.size() > 0) {
                return new Tag(hashOfTransByTag.stream().map(t -> new Hash(t.getHash())).collect(Collectors.toList()));
            } else {
                return Tag.class.newInstance();
            }
        }
    }

    @Override
    public boolean mayExist(Indexable index) throws Exception {
        // 未用
        throw new NeededException();
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

    public void saveUnUpdate(Persistable thing, Indexable index, SqlSession outSession) {
        TagMapper mapper = outSession.getMapper(TagMapper.class);
        mapper.insert((Tag) thing);
    }
}