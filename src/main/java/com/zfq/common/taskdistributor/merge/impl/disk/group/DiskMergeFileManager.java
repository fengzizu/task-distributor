package com.zfq.common.taskdistributor.merge.impl.disk.group;

import com.zfq.common.taskdistributor.merge.MergeFileManager;
import com.zfq.common.taskdistributor.merge.impl.disk.DiskMergeFile;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DiskMergeFileManager implements MergeFileManager<DiskMergeFile, Map<String, RegisterInfo>, Set<String>> {
    private final RedisTemplate redisTemplate;
    private final String mergeFileRegisterkeySuffix;
    private final String group;

    public DiskMergeFileManager(RedisTemplate redisTemplate, String mergeFileRegisterKeySuffix, String group) {
        this.redisTemplate = redisTemplate;
        this.mergeFileRegisterkeySuffix = mergeFileRegisterKeySuffix;
        this.group = group;
    }

    @Override
    public void register(DiskMergeFile mergeFile) {
        String mergeKey = mergeFile.mergeFileInfo().mergeKey();
        String registerKey = makeRegisterKey(mergeKey);
        redisTemplate.opsForHash().put(registerKey, mergeFile.getFile().getAbsolutePath(), new RegisterInfo(group, false));
    }

    @Override
    public Map<String, RegisterInfo> registerInfo(String mergeKey) {
        return redisTemplate.opsForHash().entries(makeRegisterKey(mergeKey));
    }

    @Override
    public void commit(DiskMergeFile mergeFile) {
        String mergeKey = mergeFile.mergeFileInfo().mergeKey();
        String registerKey = makeRegisterKey(mergeKey);
        DefaultRedisScript script = new DefaultRedisScript("local k = ARGV[1] local v = redis.call('HGET', KEYS[1], k) if(v) then redis.call('HSET', KEYS[1], k, ARGV[2]) return v else return nil end");
        script.setResultType(Object.class);
        this.redisTemplate.execute(script, null, redisTemplate.getValueSerializer(), List.of(registerKey),
                new Object[]{redisTemplate.getKeySerializer().serialize(mergeFile.getFile().getAbsolutePath
                        ()), redisTemplate.getValueSerializer().serialize(new RegisterInfo(this.group, true))});
    }

    @Override
    public void unregister(String mergeKey, Set<String> unregister) {
        redisTemplate.opsForHash().delete(makeRegisterKey(mergeKey), unregister.toArray());
    }

    private String makeRegisterKey(String mergeKey){
        return mergeKey + mergeFileRegisterkeySuffix;
    }

}
