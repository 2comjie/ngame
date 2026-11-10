-- KEYS[1]：当前批次所属桶的会话归属 Hash，仅包含同一桶的会话。
-- ARGV[1]：字段 TTL，单位秒。
-- ARGV[2...]：按 Uid、归属 JSON 成对传入，JSON 格式与绑定时一致。
-- 返回未匹配会话在本批次中的索引，从 1 开始；调用方据此关闭失去归属的旧会话。

local fields = {}
local lost = {}
for i = 2, #ARGV, 2 do
    -- 仅续期归属仍匹配的字段；不覆盖新归属，也不重建已过期或已解绑的记录。
    if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[i + 1] then
        fields[#fields + 1] = ARGV[i]
    else
        lost[#lost + 1] = (i - 2) / 2 + 1
    end
end

-- 将所有匹配的玩家字段一次性续期，检查与续期在同一脚本中原子执行。
if #fields > 0 then
    redis.call('HEXPIRE', KEYS[1], ARGV[1], 'FIELDS', #fields, unpack(fields))
end
return lost
