-- 必须匹配完整所有权才能续租，旧实例不能续租新实例，也不能重建过期记录
-- KEYS[1]：归属记录；ARGV[1]：完整所有权；ARGV[2]：TTL 毫秒
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
end
return 0
