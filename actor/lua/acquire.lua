-- KEYS[1]：Actor Key 对应的归属记录。
-- ARGV[1]：包含 NodeId、ActivationId 的完整所有权；ARGV[2]：TTL 毫秒。
-- 不存在才能抢占；相同所有权允许确认重试，其他激活只返回当前归属。
local owner = redis.call('GET', KEYS[1])
if not owner or owner == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return ARGV[1]
end
return owner
