-- KEYS[1]：Uid % 10 对应的会话归属 Hash，默认 {ngame:gate:桶号}:sessions。
-- ARGV[1]：玩家 Uid，作为 Hash 字段。
-- ARGV[2]：当前 GateId。
-- ARGV[3]：当前 SessionId，在同一网关进程内递增。
-- ARGV[4]：新归属 JSON，包含 gate_id、session_id。
-- ARGV[5]：字段 TTL，单位秒。
-- 返回 1 表示绑定成功；返回 0 表示拒绝迟到的同网关旧会话。

local previous = redis.call('HGET', KEYS[1], ARGV[1])
if previous then
    local current = cjson.decode(previous)
    -- 只比较同一网关的 SessionId，防止旧绑定请求迟到后覆盖新会话。
    -- 不同网关的 SessionId 不可比较；跨网关绑定按脚本执行先后覆盖。
    if current.gate_id == ARGV[2] and tonumber(current.session_id) > tonumber(ARGV[3]) then
        return 0
    end
end

redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
redis.call('HEXPIRE', KEYS[1], ARGV[5], 'FIELDS', 1, ARGV[1])
return 1
