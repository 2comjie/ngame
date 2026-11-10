-- KEYS[1]：Uid % 10 对应的会话归属 Hash。
-- ARGV[1]：玩家 Uid，作为 Hash 字段。
-- ARGV[2]：待解绑的旧归属 JSON，格式与绑定时一致。
-- 返回 1 表示已删除；返回 0 表示归属不存在或已被替换。

-- 只删除仍与旧归属完全匹配的记录，避免旧会话断开时误删新会话的绑定。
if redis.call('HGET', KEYS[1], ARGV[1]) == ARGV[2] then
    return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
