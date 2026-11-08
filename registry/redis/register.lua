if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
    return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HEXPIRE', KEYS[1], ARGV[4], 'FIELDS', 1, ARGV[1])
redis.call('HEXPIRE', KEYS[2], ARGV[4], 'FIELDS', 1, ARGV[1])
redis.call('PUBLISH', ARGV[5], ARGV[1])
return 1
