-- 仅删除仍属于本次 Gate 实例的归属，旧进程不能删除新进程的绑定。
-- KEYS[1]：归属 Hash；ARGV[1]：Uid；ARGV[2]：本次进程的 GateId。
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
    return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
