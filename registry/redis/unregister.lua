if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then
    return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('PUBLISH', ARGV[3], ARGV[1])
return 1
