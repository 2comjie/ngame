-- KEYS[1]：Uid 所属桶的归属 Hash。
-- ARGV[1]：Uid；ARGV[2]：本次进程的 GateId；ARGV[3]：TTL 秒。
-- 同一网关内的迟到绑定由当前连接对象校验，跨网关按脚本执行顺序更新。
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HEXPIRE', KEYS[1], ARGV[3], 'FIELDS', 1, ARGV[1])
return 1
