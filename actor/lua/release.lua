-- 旧租约的延迟释放不能删除同 Key 的新实例。
-- KEYS[1]：归属记录；ARGV[1]：完整所有权。
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
