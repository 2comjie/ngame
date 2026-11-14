-- KEYS[1]：当前桶的归属 Hash。
-- ARGV[1]：TTL 秒；ARGV[2]：本次进程的 GateId；ARGV[3...]：Uid。
-- 返回已失去归属的 Uid 字符串，避免 Lua 数值精度影响 uint64。
local fields = {}
local lost = {}
for i = 3, #ARGV do
    if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[2] then
        fields[#fields + 1] = ARGV[i]
    else
        lost[#lost + 1] = ARGV[i]
    end
end
-- 只续期仍属于本实例的记录，不覆盖新归属，不重建过期记录。
if #fields > 0 then
    redis.call('HEXPIRE', KEYS[1], ARGV[1], 'FIELDS', #fields, unpack(fields))
end
return lost
