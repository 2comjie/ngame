if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] or redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
    return 0
end
redis.call('HEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
redis.call('HEXPIRE', KEYS[2], ARGV[3], 'FIELDS', 1, ARGV[1])
return 1
